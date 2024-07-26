#[cfg(feature = "generate")]
mod generate {
    use std::{env, fs, path::PathBuf};

    type Result<T, E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

    pub fn etcd_proto_source() -> Result<PathBuf> {
        const ETCD_SOURCE_VAR: &str = "ETCD_GRPC_SOURCE";
        println!("cargo:rerun-if-env-changed={ETCD_SOURCE_VAR}");
        let source = match env::var(ETCD_SOURCE_VAR) {
            Ok(val) => val,
            Err(err) => panic!("set {ETCD_SOURCE_VAR}: {err}"),
        };

        Ok(PathBuf::from(source))
    }

    pub fn run() -> Result<()> {
        let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap()).join("gen-src");
        let etc_source = etcd_proto_source()?;

        if out_dir.exists() {
            fs::remove_dir_all(&out_dir)?;
        }
        fs::create_dir_all(&out_dir)?;

        tonic_build::configure()
            .emit_rerun_if_changed(true)
            .include_file("all.rs")
            .build_client(true)
            .use_arc_self(true)
            .generate_default_stubs(true)
            .disable_comments(".") // <- this doesn't work...no syntax seems to. I don't want comments. This is dumb.
            .out_dir(out_dir)
            .compile(
                &[etc_source.join("etcd/api/etcdserverpb/rpc.proto")],
                &[
                    &etc_source,
                    &etc_source.join("gogoproto"),
                    &etc_source.join("googleapis"),
                    &etc_source.join("grpc-gateway"),
                ],
            )?;

        Ok(())
    }
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    #[cfg(feature = "generate")]
    generate::run().unwrap();
}
