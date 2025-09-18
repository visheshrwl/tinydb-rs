use std::fs::{OpenOptions, File};
use std::io::{ Seek, SeekFrom, Write, Read};
use std::path::Path;
use crate::util::crc32;
use crate::wal::Lsn;

pub const PAGE_SIZE: usize = 8192;
pub type PageId = u64;


pub const HDR_SZ: usize = 28;

#[derive(Clone)]
pub struct Page {
    pub id: PageId,
    pub lsn: Lsn,
    pub used: u32,
    pub data: Vec<u8>,
}

impl Page {


    // Layout constants (explicit offsets)
    const MAGIC_OFF: usize = 0;
    const MAGIC_SZ: usize = 4;
    const ID_OFF: usize = Self::MAGIC_OFF + Self::MAGIC_SZ; // 4
    const ID_SZ: usize = 8;
    const LSN_OFF: usize = Self::ID_OFF + Self::ID_SZ; // 12
    const LSN_SZ: usize = 8;
    const USED_OFF: usize = Self::LSN_OFF + Self::LSN_SZ; // 20
    const USED_SZ: usize = 4;
    const CRC_OFF: usize = Self::USED_OFF + Self::USED_SZ; // 24
    const CRC_SZ: usize = 4;
    const HDR_SZ2: usize = Self::CRC_OFF + Self::CRC_SZ; // 28
    // We keep HDR_SZ constant at 32 as previously used; the extra 4 bytes are padding/reserved.
    // DATA starts at HDR_SZ.
    // (Keep HDR_SZ defined earlier and equal to 32.)
    // We'll use HDR_SZ constant from top-level (32).
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            lsn: 0,
            used: 0,
            data: vec![0u8; PAGE_SIZE - HDR_SZ],
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        // Create full-size buffer initialized to zeros
        let mut buf = vec![0u8; PAGE_SIZE];

        // Write header fields
        buf[Self::MAGIC_OFF..Self::MAGIC_OFF + Self::MAGIC_SZ]
            .copy_from_slice(&0xDEADBEEF_u32.to_le_bytes());
        buf[Self::ID_OFF..Self::ID_OFF + Self::ID_SZ]
            .copy_from_slice(&self.id.to_le_bytes());
        buf[Self::LSN_OFF..Self::LSN_OFF + Self::LSN_SZ]
            .copy_from_slice(&self.lsn.to_le_bytes());
        buf[Self::USED_OFF..Self::USED_OFF + Self::USED_SZ]
            .copy_from_slice(&self.used.to_le_bytes());
        // CRC slot left zero for now (CRC_OFF..CRC_OFF+CRC_SZ)

        // Write page payload into DATA region (DATA starts at HDR_SZ)
        let data_start = HDR_SZ;
        if self.data.len() != PAGE_SIZE - HDR_SZ {
            // ensure invariant
            panic!("page.data length mismatch: {} != {}", self.data.len(), PAGE_SIZE - HDR_SZ);
        }
        buf[data_start..PAGE_SIZE].copy_from_slice(&self.data);

        // Build CRC source: header bytes excluding the CRC slot (0..CRC_OFF)
        // concatenated with the data region (DATA_START .. PAGE_SIZE)
        let mut crc_src = Vec::with_capacity(Self::CRC_OFF + (PAGE_SIZE - HDR_SZ));
        crc_src.extend_from_slice(&buf[0..Self::CRC_OFF]); // magic,id,lsn,used
        crc_src.extend_from_slice(&buf[data_start..PAGE_SIZE]); // data

        let crc = crc32(&crc_src);
        buf[Self::CRC_OFF..Self::CRC_OFF + Self::CRC_SZ].copy_from_slice(&crc.to_le_bytes());

        // final sanity: buffer length == PAGE_SIZE
        assert_eq!(buf.len(), PAGE_SIZE);
        buf
    }

    pub fn from_bytes(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != PAGE_SIZE {
            return Err(anyhow::anyhow!("page size mismatch (expected {}, got {})", PAGE_SIZE, b.len()));
        }

        // Validate magic
        let magic = u32::from_le_bytes(b[Self::MAGIC_OFF..Self::MAGIC_OFF + Self::MAGIC_SZ].try_into().unwrap());
        if magic != 0xDEADBEEF {
            return Err(anyhow::anyhow!("bad page magic: {:08x}", magic));
        }

        let id = u64::from_le_bytes(b[Self::ID_OFF..Self::ID_OFF + Self::ID_SZ].try_into().unwrap());
        let lsn = u64::from_le_bytes(b[Self::LSN_OFF..Self::LSN_OFF + Self::LSN_SZ].try_into().unwrap());
        let used = u32::from_le_bytes(b[Self::USED_OFF..Self::USED_OFF + Self::USED_SZ].try_into().unwrap());
        let crc_stored = u32::from_le_bytes(b[Self::CRC_OFF..Self::CRC_OFF + Self::CRC_SZ].try_into().unwrap());

        // Extract data
        let mut data = vec![0u8; PAGE_SIZE - HDR_SZ];
        data.copy_from_slice(&b[HDR_SZ..PAGE_SIZE]);

        // Recompute CRC over same bytes we used in to_bytes
        let mut crc_src = Vec::with_capacity(Self::CRC_OFF + (PAGE_SIZE - HDR_SZ));
        crc_src.extend_from_slice(&b[0..Self::CRC_OFF]);
        crc_src.extend_from_slice(&b[HDR_SZ..PAGE_SIZE]);
        let crc_calc = crc32(&crc_src);

        if crc_calc != crc_stored {
            // Print helpful debug — hex dump of first 64 bytes and the CRC mismatch
            eprintln!("PAGE CRC MISMATCH for page id={}", id);
            eprintln!("  stored_crc = {:#010x}", crc_stored);
            eprintln!("  calc_crc   = {:#010x}", crc_calc);
            // show first 64 bytes of buffer in hex for inspection
            let show_n = 64.min(b.len());
            eprintln!("first {} bytes:", show_n);
            for chunk in b[0..show_n].chunks(16) {
                eprint!("  ");
                for byte in chunk { eprint!("{:02x} ", byte); }
                eprintln!();
            }
            return Err(anyhow::anyhow!("page crc mismatch id={}", id));
        }

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
        self.file.flush()?;
        Ok(())
    }

    pub fn sync(&mut self) -> anyhow::Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}