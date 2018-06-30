extern crate quick_xml;

use flate2::bufread::DeflateDecoder;
use quick_xml::events::Event;
use quick_xml::Reader;
use reader::DoubleBufferReader;
use std::fs::File;
use std::io;
use std::io::{BufReader, Read, Write};
use std::str::from_utf8;
use std::sync::Arc;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::RwLock;
use std::thread;


pub fn parser_worker(b1: Arc<RwLock<Vec<u8>>>, b2: Arc<RwLock<Vec<u8>>>, logger: Sender<String>) {
    let b1 = b1.read().unwrap();
    let b2 = b2.read().unwrap();
    let dbr = DoubleBufferReader::new(&b1, &b2);
    let br = BufReader::new(dbr);

    let mut xml = Reader::from_reader(br);
    let mut count = 0;
    let mut buf = vec![0; 20000];
    loop {
        match xml.read_event(&mut buf) {
            Ok(Event::Start(ref e)) if e.name() == "ns:Statistik".as_bytes() => count += 1,
            Ok(Event::Eof) => break,
            Err(e) => println!("{:?}", e),
            _ => {}
        }
        buf.clear();
    }
    logger.send(format!("Statistik tags: {}", count));
}
