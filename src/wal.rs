use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write, Read};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::util::crc32;

pub type Lsn = u64;

/*
Simple  WAL File with append, fsync and sequential replay
*/

pub struct Wal{
    file: Arc<Mutex<File>>,
    next_lsn: Arc<Mutex<Lsn>>,
}

impl Wal {
    pub fn open<P: AsRef<Path>>(path: P) -> anyhow::Result<Self>{
        let f = OpenOptions::new().create(true).append(true).read(true).open(path)?;
        let mut reader = f.try_clone()?;
        let next = compute_next_lsn(&mut reader)?;
        Ok(Self {file: Arc::new(Mutex::new(f)), next_lsn: Arc::new(Mutex::new(next)) })
    }

    pub fn append(&self, payload: &[u8]) -> anyhow::Result<Lsn> {
        let mut f = self.file.lock().unwrap();
        let mut lsn_g = self.next_lsn.lock().unwrap();
        let lsn = *lsn_g;
        // construct record
        let crc = crc32(payload);
        let total_len = 8 + 4 + (payload.len() as u64); // lsn(8) + crc(4) + payload
        f.write_all(&total_len.to_le_bytes())?;
        f.write_all(&lsn.to_le_bytes())?;
        f.write_all(&crc.to_le_bytes())?;
        f.write_all(payload)?;
        *lsn_g += 1;
        Ok(lsn)
    }

    pub fn sync(&self) -> anyhow::Result<()> {
        let f = self.file.lock().unwrap();
        f.sync_all()?;
        Ok(())
    }
    
    pub fn replay_from_start<P: AsRef<Path>> (path:P, mut visitor: impl FnMut(Lsn, Vec<u8>) -> anyhow::Result<()>) -> anyhow::Result<()> {
        let mut f = File::open(path)?;
        f.seek(SeekFrom::Start(0))?;
        loop {
            let mut lenb = [0u8; 8];
            if f.read_exact(&mut lenb).is_err(){break;}
            let total_len = u64::from_le_bytes(lenb);
            let mut lsnb = [0u8; 8];
            f.read_exact(&mut lsnb)?;
            let lsn = u64::from_le_bytes(lsnb);
            let mut crc_b = [0u8; 4];
            f.read_exact(&mut crc_b)?;
            let crc = u32::from_le_bytes(crc_b);
            let payload_len = total_len - 12;
            let mut payload = vec![0u8; payload_len as usize];
            f.read_exact(&mut payload)?;
            if crc32(&payload) != crc { return Err(anyhow::anyhow!("WAL Payload CRC Mismatch at LSN {}", lsn)); }
            visitor(lsn, payload)?;
        }
        Ok(())
    }
}

fn compute_next_lsn(f: &mut File) -> anyhow::Result<Lsn>{
    f.seek(SeekFrom::Start(0))?;
    let mut next = 0u64;
    loop {
        let mut lenb = [0u8;8];
        if f.read_exact(&mut lenb).is_err(){break; }
        let total_len = u64::from_le_bytes(lenb);
        let mut lsnb = [0u8; 8];
        f.read_exact(&mut lsnb)?;
        let lsn = u64::from_le_bytes(lsnb);
        f.seek(SeekFrom::Current(4+(total_len as i64 - 12)))?;
        next = lsn +1;
    }
    Ok(next)
}