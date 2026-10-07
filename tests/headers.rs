//! The C header the extension includes is generated from the FFI in src/lib.rs
//! (cbindgen.toml), so Rust and C++ cannot disagree about a signature: a
//! mismatch compiles on both sides and crashes at run time.
//!
//! `just headers` regenerates it.

#[test]
fn pgvfs_h_matches_the_ffi() {
    let root = env!("CARGO_MANIFEST_DIR");
    let config = cbindgen::Config::from_file(format!("{root}/cbindgen.toml")).unwrap();
    let mut generated = Vec::new();
    cbindgen::Builder::new()
        .with_crate(root)
        .with_config(config)
        .generate()
        .expect("cbindgen")
        .write(&mut generated);
    let path = format!("{root}/extension/src/include/pgvfs.h");
    if std::env::var_os("UPDATE_HEADERS").is_some() {
        std::fs::write(&path, &generated).unwrap();
        return;
    }
    let current = std::fs::read(&path).unwrap_or_default();
    assert!(
        current == generated,
        "{path} is out of date: run `just headers`"
    );
}
