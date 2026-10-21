fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut compiler = protox::Compiler::new(["proto"])?;
    compiler
        .include_source_info(true)
        .include_imports(true)
        .open_files([
            "google/cloud/tasks/v2/cloudtasks.proto",
            "google/rpc/error_details.proto",
        ])?;
    let out_dir = std::env::var("OUT_DIR")?;
    std::fs::write(
        format!("{out_dir}/descriptors.bin"),
        compiler.encode_file_descriptor_set(),
    )?;
    tonic_prost_build::configure()
        .btree_map(".")
        .compile_fds(compiler.file_descriptor_set())?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
