extern crate flate2;
extern crate quick_xml;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use std::env;
use std::str::FromStr;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::io;
use std::io::Write;
use worker::process_file;

const BUFFER_COUNT_DEFAULT: usize = 8;
const BUFFER_SIZE_DEFAULT: usize = 150_000_000;

fn get_usize_env_or(key: &str, default: usize) -> usize {
    match env::var(key) {
        Ok(v) => {
            match usize::from_str(&v) {
                Ok(n) => n,
                Err(_) => default,
            }
        }
        Err(_) => default,
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: dmrparse filename.zip");
        return;
    }

    let filename = &args[1];

    let (log_tx, log_rx): (Sender<String>, Receiver<String>) = channel();
    let log_join = thread::spawn(move || {
        let stdout = io::stdout();
        let mut stdout_handle = stdout.lock();
        while let Ok(s) = log_rx.recv() {
            stdout_handle.write(format!("{}\n", s).as_bytes()).unwrap();
        }
    });

    let buffer_count = get_usize_env_or("DMR_PARSE_BUFFER_COUNT", BUFFER_COUNT_DEFAULT);
    let buffer_size = get_usize_env_or("DMR_PARSE_BUFFER_SIZE", BUFFER_SIZE_DEFAULT);

    let _ = process_file(filename, log_tx, buffer_count, buffer_size).unwrap();

    //wait for logging to complete
    log_join.join().unwrap();
}

mod reader;
mod worker;
