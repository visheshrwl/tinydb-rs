#[allow(unused_variables)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::wal::Wal;
use crate::pager::{Pager, Page, PAGE_SIZE};
use crate::wal::Lsn;
use crate::util::crc32;

/// Very small single-file KV engine on top of pages.
/// Layout: each page stores multiple kvs as:
/// [u32: key_len][u32: val_len][key..][val..] repeated
/// We keep a small in-memory index mapping key -> (page_id, offset, val_len).
///
/// WAL payload types: simple encoded op:
/// "SET"<u64 page_id><u32 off><u32 key_len><u32 val_len><key><val>
/// For simplicity we allocate a new page when current doesn't fit; no deletion compaction.
use anyhow::Context;

const WAL_FILE: &str = "tinydb_wal.log";
const DATA_FILE: &str = "tinydb_data.db";

pub struct Engine {
    wal: Arc<Wal>,
    pager: Arc<Mutex<Pager>>,
    // in-memory index
    index: Arc<Mutex<HashMap<String, (u64, u32, u32)>>>,
    // next page to append
    next_page: Arc<Mutex<u64>>,
}

impl Engine {
    pub fn open<P: AsRef<Path>>(dir: P) -> anyhow::Result<Self> {
        let mut dirp = dir.as_ref().to_path_buf();
        dirp.push(WAL_FILE);
        let wal = Arc::new(Wal::open(&dirp).context("open wal")?);

        let mut datap = dir.as_ref().to_path_buf();
        datap.push(DATA_FILE);
        let pager = Arc::new(Mutex::new(Pager::open(&datap).context("open pager")?));

        // simple: reconstruct index by scanning all pages and reading kvs.
        let mut idx = HashMap::new();
        let mut next_page = 0u64;
        {
            let mut p = pager.lock().unwrap();
            // naive scan: read pages until read_page returns Page::new (empty)
            loop {
                let page = p.read_page(next_page)?;
                // if page is new (lsn==0 and used==0) break
                if page.used == 0 && page.lsn == 0 {
                    break;
                }
                // parse kvs
                let mut off = 0usize;
                let payload = &page.data;
                while off + 12 <= payload.len() {
                    let key_len = u32::from_le_bytes(payload[off..off+4].try_into().unwrap()) as usize;
                    let val_len = u32::from_le_bytes(payload[off+4..off+8].try_into().unwrap()) as usize;
                    let total = 8 + key_len + val_len;
                    if key_len == 0 || off + total > payload.len() { break; }
                    let key = String::from_utf8_lossy(&payload[off+8..off+8+key_len]).to_string();
                    // store location
                    idx.insert(key, (next_page, off as u32, val_len as u32));
                    off += total;
                }
                next_page += 1;
            }
        }

        let engine = Self {
            wal,
            pager,
            index: Arc::new(Mutex::new(idx)),
            next_page: Arc::new(Mutex::new(next_page)),
        };

        // Replay WAL from start to ensure we incorporate recent changes (recovery)
        let mut walpath = dir.as_ref().to_path_buf();
        walpath.push(WAL_FILE);
        Wal::replay_from_start(&walpath, |lsn, payload| {
            // decode payload: first 3 bytes are type ascii "SET"
            if payload.len() < 3 { return Ok(()); }
            let t = &payload[0..3];
            if t == b"SET" {
                // parse
                let mut off = 3;
                let page_id = u64::from_le_bytes(payload[off..off+8].try_into().unwrap()); off += 8;
                let offset = u32::from_le_bytes(payload[off..off+4].try_into().unwrap()); off += 4;
                let key_len = u32::from_le_bytes(payload[off..off+4].try_into().unwrap()) as usize; off += 4;
                let val_len = u32::from_le_bytes(payload[off..off+4].try_into().unwrap()) as usize; off += 4;
                let key = String::from_utf8_lossy(&payload[off..off+key_len]).to_string(); off += key_len;
                let val = &payload[off..off+val_len];
                // apply into pager
                let mut pg = engine.pager.lock().unwrap();
                // ensure page exists
                let mut page = pg.read_page(page_id)?;
                // write kv bytes into page.data at offset
                let dest_off = offset as usize;
                // re-encode the kv entry: key_len u32, val_len u32, key, val
                let mut entry = Vec::with_capacity(8 + key_len + val_len);
                entry.extend_from_slice(&(key_len as u32).to_le_bytes());
                entry.extend_from_slice(&(val_len as u32).to_le_bytes());
                entry.extend_from_slice(key.as_bytes());
                entry.extend_from_slice(val);
                page.data[dest_off..dest_off+entry.len()].copy_from_slice(&entry);
                page.used = page.used.max((dest_off + entry.len()) as u32);
                page.lsn = lsn;
                pg.write_page(&page)?;
                // update in-memory index
                engine.index.lock().unwrap().insert(key, (page_id, dest_off as u32, val_len as u32));
            }
            Ok(())
        })?;

        // Note: no checkpointing on open here; in a real system you'd examine WAL LSN and pageLSNs.
        Ok(engine)
    }

    /// single-writer SET. Steps:
    /// 1) find a page & offset to store kv (simple append)
    /// 2) build WAL payload describing SET with page/offset/key/val
    /// 3) append WAL -> get LSN
    /// 4) sync WAL (fsync)
    /// 5) apply to page in-memory and write page (lazy flush could be later; here we write immediately for simplicity)
    pub fn set(&mut self, key: &str, val: &[u8]) -> anyhow::Result<()> {
        // encode entry
        let key_b = key.as_bytes();
        let key_len = key_b.len();
        let val_len = val.len();
        let entry_len = 8 + key_len + val_len; // keylen+vallen header + key + val

        // find page with enough space
        let mut pid = *self.next_page.lock().unwrap();
        let mut page = {
            let mut p = self.pager.lock().unwrap();
            let mut page = p.read_page(pid)?;
            if (PAGE_SIZE - pager_hdr_sz()) < (page.used as usize + entry_len) {
                // allocate new page
                pid += 1;
                *self.next_page.lock().unwrap() = pid;
                page = Page::new(pid);
            }
            page
        };

        // offset where kv will be written
        let off = page.used as usize;
        // craft WAL payload
        // payload = b"SET" + page_id(8) + offset(4) + key_len(4) + val_len(4) + key + val
        let mut payload = Vec::with_capacity(3 + 8 + 4 + 4 + 4 + key_len + val_len);
        payload.extend_from_slice(b"SET");
        payload.extend_from_slice(&pid.to_le_bytes());
        payload.extend_from_slice(&(off as u32).to_le_bytes());
        payload.extend_from_slice(&(key_len as u32).to_le_bytes());
        payload.extend_from_slice(&(val_len as u32).to_le_bytes());
        payload.extend_from_slice(key_b);
        payload.extend_from_slice(val);

        // append wal
        let lsn = self.wal.append(&payload)?;
        self.wal.sync()?; // fsync the WAL before ack

        // apply to page and write page to disk
        {
            let mut pgr = self.pager.lock().unwrap();
            let mut page = pgr.read_page(pid)?;
            // recompose entry
            let mut entry = Vec::with_capacity(8 + key_len + val_len);
            entry.extend_from_slice(&(key_len as u32).to_le_bytes());
            entry.extend_from_slice(&(val_len as u32).to_le_bytes());
            entry.extend_from_slice(key_b);
            entry.extend_from_slice(val);
            page.data[off..off+entry.len()].copy_from_slice(&entry);
            page.used = (off + entry.len()) as u32;
            page.lsn = lsn;
            pgr.write_page(&page)?;
            pgr.sync()?;
            // update index
            self.index.lock().unwrap().insert(key.to_string(), (pid, off as u32, val_len as u32));
        }

        Ok(())
    }

    pub fn get(&mut self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        if let Some((pid, off, val_len)) = self.index.lock().unwrap().get(key).cloned() {
            let mut p = self.pager.lock().unwrap();
            let page = p.read_page(pid)?;
            let off = off as usize;
            let key_len = u32::from_le_bytes(page.data[off..off+4].try_into().unwrap()) as usize;
            let val_len = u32::from_le_bytes(page.data[off+4..off+8].try_into().unwrap()) as usize;
            let val_start = off + 8 + key_len;
            let val = page.data[val_start..val_start+val_len].to_vec();
            return Ok(Some(val));
        }
        Ok(None)
    }
}

fn pager_hdr_sz() -> usize {
    // PAGE_SIZE - data len = hdr
    PAGE_SIZE - (PAGE_SIZE - 32)
}
