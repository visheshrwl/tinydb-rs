use std::fs::{OpenOptions, File};
use std::io::{ Seek, SeekFrom, Write, Read};
use std::path::Path;
use crate::util::crc32;
use crate::wal::Lsn;

pub const PAGE_SIZE: usize = 8192;
pub type PageId = u64;


const HDR_SZ: usize = 32;

#[derive(Clone)]
pub struct Page {
    pub id: PageId,
    pub lsn: Lsn,
    pub used: u32,
    pub data: Vec<u8>,
}

impl Page {
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            lsn: 0,
            used: 0,
            data: vec![0; PAGE_SIZE-HDR_SZ],
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(PAGE_SIZE);
        v.extend_from_slice(&u32::to_le_bytes(0xDEADBEEF));
        v.extend_from_slice(&self.id.to_le_bytes());
        v.extend_from_slice(&self.lsn.to_le_bytes());
        v.extend_from_slice(&self.used.to_le_bytes());
        v.extend_from_slice(&[0u8; 4]);
        v.extend_from_slice(&self.data);
        let crc = crc32(&v[0..(HDR_SZ-4) + (PAGE_SIZE - HDR_SZ)]);
        let crc_off = 4+8+8+4;

        v[crc_off..crc_off+4].copy_from_slice(&crc.to_le_bytes());
        v.resize(PAGE_SIZE, 0u8);
        v
    }

    pub fn from_bytes(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != PAGE_SIZE { return Err(anyhow::anyhow!("page size mismatch")); }
        let magic = u32::from_le_bytes([b[0],b[1],b[2],b[3]]);
        if magic != 0xDEADBEEF { return Err(anyhow::anyhow!("bad page magic")); }
        let id = u64::from_le_bytes(b[4..12].try_into().unwrap());
        let lsn = u64::from_le_bytes(b[12..20].try_into().unwrap());
        let used = u32::from_le_bytes(b[20..24].try_into().unwrap());
        let crc = u32::from_le_bytes(b[24..28].try_into().unwrap());
        let mut data = vec![0u8; PAGE_SIZE - HDR_SZ];
        data.copy_from_slice(&b[HDR_SZ..PAGE_SIZE]);
        // verify crc
        let mut v = Vec::with_capacity(PAGE_SIZE);
        v.extend_from_slice(&b[0..(HDR_SZ-4)]);
        v.extend_from_slice(&b[HDR_SZ..PAGE_SIZE]);
        let calc = crc32(&v);
        if calc != crc { return Err(anyhow::anyhow!("page crc mismatch id={}", id)); }
        Ok(Self { id, lsn, used, data })
    }
}

pub struct Pager {
    file: File,
}

impl Pager {
    pub fn open<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let f = OpenOptions::new().create(true).read(true).write(true).open(path)?;
        Ok(Self { file: f })
    }

    pub fn read_page(&mut self, pid: PageId) -> anyhow::Result<Page> {
        let off = pid as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(off))?;
        let mut buf = vec![0u8; PAGE_SIZE];
        let n = self.file.read(&mut buf)?;
        if n == 0 {
            // not present: return empty page
            return Ok(Page::new(pid));
        }
        if n != PAGE_SIZE {
            return Err(anyhow::anyhow!("short read {} != {}", n, PAGE_SIZE));
        }
        Page::from_bytes(&buf)
    }

    pub fn write_page(&mut self, page: &Page) -> anyhow::Result<()> {
        let off = page.id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(off))?;
        let b = page.to_bytes();
        self.file.write_all(&b)?;
        Ok(())
    }

    pub fn sync(&mut self) -> anyhow::Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}