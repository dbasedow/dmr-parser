extern crate flate2;
extern crate quick_xml;
extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

use flate2::bufread::DeflateDecoder;
use quick_xml::events::Event;
use quick_xml::Reader;
use reader::DoubleBufferReader;
use worker::parser_worker;
use std::fs::File;
use std::io;
use std::io::{BufReader, Read, Write};
use std::str::from_utf8;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::sync::Arc;
use std::sync::RwLock;
use std::io::BufRead;
use std::env;
use std::str::FromStr;

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
        while let Ok(s) = log_rx.recv() {
            println!("{}", s);
        }
    });

    let buffer_count = get_usize_env_or("DMR_PARSE_BUFFER_COUNT", BUFFER_COUNT_DEFAULT);
    let buffer_size = get_usize_env_or("DMR_PARSE_BUFFER_SIZE", BUFFER_SIZE_DEFAULT);

    eprintln!("Buffer count: {} size: {}", buffer_count, buffer_size);
    process_file(filename, log_tx, buffer_count, buffer_size);

    //wait for logging to complete
    log_join.join();
}

fn fill_buffer<T: Read>(buf: &mut [u8], rdr: &mut T) -> Result<usize, io::Error> {
    let mut offset = 0;
    loop {
        if offset == buf.len() {
            return Ok(offset);
        }
        match rdr.read(&mut buf[offset..]) {
            Ok(0) => return Ok(offset),
            Ok(n) => offset += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

fn process_zip_header(f: &mut BufReader<File>) {
    let mut header_buf = [0; 30];

    f.read_exact(&mut header_buf)
        .expect("unable to read header");

    assert_eq!(header_buf[..4], [0x50, 0x4b, 0x03, 0x04]);
    let name_len = ((header_buf[27] as usize) << 8) + header_buf[26] as usize;
    let extra_len = ((header_buf[29] as usize) << 8) + header_buf[28] as usize;

    let mut name_extra_buf = vec![0; name_len + extra_len];
    f.read_exact(&mut name_extra_buf);
}

fn process_file(filename: &str, log_tx: Sender<String>, buffer_count: usize, buffer_size: usize) {
    let f = File::open(filename).unwrap();
    let mut f = BufReader::new(f);

    process_zip_header(&mut f);

    let mut deflater = DeflateDecoder::new(f);

    let mut bufs = Vec::with_capacity(buffer_count);
    for _ in 0..buffer_count {
        bufs.push(Arc::new(RwLock::new(vec![0; buffer_size])));
    }

    //we need two buffers to start handing buffer pairs off to threads. create one now, so the
    //loop doesn't have to care
    {
        let mut b = bufs[0].write().unwrap();
        fill_buffer(&mut b, &mut deflater);
    }
    let mut count = 1;
    let mut index;
    let mut prev_index;
    let mut completed = false;

    loop {
        index = count % buffer_count;
        prev_index = (count - 1) % buffer_count;
        // scope for write lock
        {
            let mut b = bufs[index].write().unwrap();
            if let Ok(n) = fill_buffer(&mut b, &mut deflater) {
                if n < b.len() {
                    b.truncate(n);
                    completed = true;
                }
            }
        }
        let buf1 = bufs[prev_index].clone();
        let buf2 = bufs[index].clone();
        let logger = log_tx.clone();
        thread::spawn(move || {
            parser_worker(buf1, buf2, logger);
        });

        count += 1;

        if completed {
            // spawn last thread
            let next_index = (count + 1) % buffer_count;
            {
                let mut b = bufs[next_index].write().unwrap();
                b.truncate(0);
            }
            let buf1 = bufs[index].clone();
            let buf2 = bufs[next_index].clone();
            let logger = log_tx.clone();
            thread::spawn(move || {
                parser_worker(buf1, buf2, logger);
            });
            break;
        }
    }

    // go through all buffers and try to acquire write lock. these calls block until no readers are
    // left. that way we know all threads have finished.
    for i in 0..buffer_count {
        bufs[i].write();
    }
}

mod reader;
mod worker;
