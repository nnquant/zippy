use std::io::{self, Read};

use sha2::{Digest, Sha256};

pub(crate) const SHA256_V1_PREFIX: &str = "sha256-v1:";

pub(crate) fn sha256_v1_reader(mut reader: impl Read) -> io::Result<String> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{SHA256_V1_PREFIX}{:x}", hasher.finalize()))
}
