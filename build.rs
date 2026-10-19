fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptors = protox::compile(["google/cloud/tasks/v2/cloudtasks.proto"], ["proto"])?;
    tonic_prost_build::configure()
        .btree_map(".")
        .compile_fds(descriptors)?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
