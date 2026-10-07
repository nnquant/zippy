//! Generate Rust protocol types from the same descriptor used for Python generation.

use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let descriptor = args.next().ok_or("missing descriptor path")?;
    let output = args.next().ok_or("missing output directory")?;
    if args.next().is_some() {
        return Err("unexpected generator argument".into());
    }
    let bytes = std::fs::read(descriptor)?;
    let descriptors = prost_types::FileDescriptorSet::decode(bytes.as_slice())?;
    let mut config = prost_build::Config::new();
    config.bytes(["."]);
    config.out_dir(output);
    config.compile_fds(descriptors)?;
    Ok(())
}
