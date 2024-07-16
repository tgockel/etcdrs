#[cfg(feature = "generate")]
include!(concat!(env!("OUT_DIR"), "/gen-src/all.rs"));

#[cfg(not(feature = "generate"))]
include!(concat!(env!("CARGO_MANIFEST_DIR"), "/gen-src/all.rs"));
