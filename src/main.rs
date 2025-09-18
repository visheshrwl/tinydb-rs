mod dev_tests;

use std::env;
use std::path::PathBuf;

mod wal;
mod pager;
mod engine;
mod util;

use engine::Engine;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Usage: {} <cmd> [args]\n cmds: set|get|recovery|run_tests",
        args[0]);
        return Ok(());
    }

    let mut data_dir = PathBuf::from("./tinydb_data");
    if let Ok(dir) = env::var("TINYDB_DATA_DIR") {
        data_dir = PathBuf::from(dir);
    }
    std::fs::create_dir_all(&data_dir)?;

    let cmd = args[1].as_str();
    match cmd{
        "set" => {
            if args.len() != 4 {
                println!("Usage : set <key> <value>"); return Ok(());
            }
            let key = args[2].clone();
            let value = args[3].clone();
            let mut db = Engine::open(&data_dir)?;
            db.set(&key, value.as_bytes())?;
            println!("OK");
        }
        "get" => {
            if args.len() != 3 {
                println!("Usage : get <key>"); return Ok(());
            }
            let key = args[2].clone();
            let mut db = Engine::open(&data_dir)?;
            match db.get(&key)? {
                Some(v) => println!("Value: {}", String::from_utf8_lossy(&v)),
                None => println!("Not found"),
            }
        }
        "recovery" => {
            let _db = Engine::open(&data_dir)?;
            println!("Recovery complete");
        }
        "run_tests" => {
            dev_tests::simple_crash_recovery()?;
            println!("Tests passed");
        }
        _ => println!("Unknown Command {}", cmd),
    }
    Ok(())
}