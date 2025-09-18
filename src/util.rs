#[allow(dead_code)]

use std::io::Read;

pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xffffffff;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            if (crc & 1) != 0 {
                crc = (crc >> 1) ^ 0xedb88320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

pub fn read_all<R: Read> (r: &mut R) -> std::io::Result<Vec<u8>>{
    let mut b = Vec::new();
    r.read_to_end(&mut b)?;
    Ok(b)
}