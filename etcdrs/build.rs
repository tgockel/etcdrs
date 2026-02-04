#[cfg(feature = "generate")]
mod generate {
    use std::{
        env,
        ffi::OsStr,
        fs,
        io::Write,
        path::{Path, PathBuf},
    };

    type Result<T, E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

    /// The `ETCD_GRPC_SOURCE` variable should be pointed at a directory that has all of the relevant repositories
    /// checked out:
    ///
    /// ```sh
    /// git clone git@github.com:etcd-io/etcd.git
    /// git clone git@github.com:googleapis/googleapis.git
    /// git clone git@github.com:grpc-ecosystem/grpc-gateway.git
    /// git clone git@github.com:gogo/protobuf.git gogoproto
    /// ```
    ///
    /// This might be an appropriate use for submodules.
    pub fn etcd_proto_source() -> Result<PathBuf> {
        const ETCD_SOURCE_VAR: &str = "ETCD_GRPC_SOURCE";
        println!("cargo:rerun-if-env-changed={ETCD_SOURCE_VAR}");
        let source = match env::var(ETCD_SOURCE_VAR) {
            Ok(val) => val,
            Err(err) => panic!("set {ETCD_SOURCE_VAR}: {err}"),
        };

        Ok(PathBuf::from(source))
    }

    /// Some hackery because I could not figure out the proper way to do these things with `tonic-build`. See the inside
    /// of the function for the things that are getting replaced.
    fn fix_file(path: &Path) -> Result<()> {
        let source = fs::read_to_string(path)?;
        let is_aggregate = path.file_name().unwrap() == "all.rs";

        let mut output = fs::File::create(path)?;
        for line in source.lines() {
            // # Strip Comments
            // There is theoretically a `disable_comments` feature on Tonic build configuration, but I can't seem to
            // make it work. Having comments interferes with `doctest` and since none of the generated files are public,
            // there is no value to keeping the comments.
            if line.trim_start().starts_with("///") {
                continue;
            }

            let need_allow_unused = is_aggregate && line.starts_with("pub mod ");
            // # `pub(crate)` instead of `pub`
            // To work with the public dependency checker (at least as of 1.82 nightly), structures and functions which
            // are marked `pub` are public dependencies, even if the module they are in is not public. This seems to be
            // a bug. But to work around it for now, we just replace everything with `pub(crate)`.
            // `cargo +nightly build -Zpublic-dependency`
            writeln!(output, "{}", line.replace("pub ", "pub(crate) "))?;
            if need_allow_unused {
                // # Allowing Unused Code
                // Tonic and Prost generate a lot of code, but we do not use all of it. The compiler gives a warning for
                // this (since we hacked `pub(crate)` on things), so tell it that we're okay.
                writeln!(output, "    #![allow(unused)]")?;
            }
        }
        output.flush().map_err(Into::into)
    }

    fn should_save_generated_files() -> bool {
        const SAVE_GENERATED_FILES_VAR: &str = "ETCDRS_SAVE_GENERATED_FILES";
        println!("cargo:rerun-if-env-changed={SAVE_GENERATED_FILES_VAR}");
        let Some(var) = env::var_os(SAVE_GENERATED_FILES_VAR) else {
            return false;
        };
        var.to_str().unwrap() == "1"
    }

    fn set_protoc_path() -> Result<()> {
        println!("cargo:rerun-if-env-changed=PROTOC");

        if env::var_os("PROTOC").is_none() {
            env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
        }

        Ok(())
    }

    pub fn run() -> Result<()> {
        set_protoc_path().expect("could not set protoc path");

        let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap()).join("gen-src");
        let etc_source = etcd_proto_source()?;

        if out_dir.exists() {
            fs::remove_dir_all(&out_dir)?;
        }
        fs::create_dir_all(&out_dir)?;

        tonic_prost_build::configure()
            .emit_rerun_if_changed(true)
            .include_file("all.rs")
            .build_client(true)
            .use_arc_self(true)
            .generate_default_stubs(true)
            .out_dir(&out_dir)
            .compile_protos(
                &[etc_source.join("etcd/api/etcdserverpb/rpc.proto")],
                &[
                    etc_source.clone(),
                    etc_source.join("gogoproto"),
                    etc_source.join("googleapis"),
                    etc_source.join("grpc-gateway"),
                ],
            )?;

        let generated_files: Vec<_> = fs::read_dir(&out_dir)?
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(OsStr::to_str) == Some("rs"))
            .map(|entry| entry.path())
            .collect();

        for gen_file in generated_files.iter() {
            fix_file(gen_file)?;
        }

        if should_save_generated_files() {
            let gen_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap()).join("gen-src");
            if gen_dir.exists() {
                fs::remove_dir_all(&gen_dir).expect("could not delete directory");
            }
            fs::create_dir_all(&gen_dir).expect("could not create generated output directory");
            for src_file in generated_files {
                fs::copy(&src_file, gen_dir.join(src_file.file_name().unwrap())).unwrap();
            }
        }

        Ok(())
    }
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    #[cfg(feature = "generate")]
    generate::run().unwrap();
}
