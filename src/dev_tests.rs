use std::process::{Command};
use std::fs;

pub fn simple_crash_recovery() -> anyhow::Result<()> {
    let dir = std::path::PathBuf::from("./tinydb_data_test");
    if dir.exists(){
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    let exe = std::env::current_exe()?;
    let status = Command::new(exe).
        arg("set")
        .arg("key1")
        .arg("value1")
        .env("TINYDB_DATA_DIR", &dir)
        .status()?;
    assert!(status.success());

    let mut db = crate::engine::Engine::open(&dir)?;
    let v = db.get("key1")?.expect("key1 should exist after recovery");
    assert_eq!(v, b"value1");
    Ok(())
}