extern crate flate2;
extern crate quick_xml;

use flate2::bufread::DeflateDecoder;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Read};
use std::str::from_utf8;
//use xml::reader::{EventReader, XmlEvent};
use quick_xml::Reader;
use quick_xml::events::Event;
use reader::{DoubleBufferReader};

fn fill_buffer<T: Read>(buf: &mut [u8], rdr: &mut T) -> Result<usize, io::Error> {
    let mut offset = 0;
    loop {
        println!("{}", offset);
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

fn main() {
    let f = File::open("ESStatistikListeModtag-20180610-200409.zip").unwrap();
    let mut f = BufReader::new(f);
    let mut header_buf = [0; 30];

    f.read_exact(&mut header_buf)
        .expect("unable to read header");

    assert_eq!(header_buf[..4], [0x50, 0x4b, 0x03, 0x04]);
    let name_len = ((header_buf[27] as usize) << 8) + header_buf[26] as usize;
    let extra_len = ((header_buf[29] as usize) << 8) + header_buf[28] as usize;

    let mut name_extra_buf = vec![0; name_len + extra_len];
    f.read_exact(&mut name_extra_buf);
    println!(
        "parsing {}",
        from_utf8(&name_extra_buf[..name_len]).unwrap()
    );

    let mut deflater = DeflateDecoder::new(f);
    //let mut deflater = BufReader::new(deflater);

    let mut buf1 = vec![0; 100_000_000];
    let mut buf2 = vec![0; 100_000_000];

    //TODO: try!
    if let Ok(n) = fill_buffer(&mut buf1, &mut deflater) {
        println!("ok: {}", n);
    }
    if let Ok(n) = fill_buffer(&mut buf2, &mut deflater) {
        println!("ok: {}", n);
    }
    let dbr = DoubleBufferReader::new(&buf1, &buf2);
    let mut br = BufReader::new(dbr);

    let mut xml = Reader::from_reader(br);
    let mut count = 0;
    let mut buf = vec![0; 10000];
    loop {
        match xml.read_event(&mut buf) {
            Ok(Event::Start(ref e)) if e.name() == "ns:Statistik".as_bytes() => count += 1,
            Ok(Event::Eof) => break,
            _ => {},
        }
        buf.clear();
    }
    println!("Statistik tags: {}", count);
    /*
    let parser = EventReader::new(deflater);

    let mut count = 0;
    for e in parser {
        match e {
            Ok(XmlEvent::StartElement {ref name, ..}) if name.local_name == "Statistik" => count += 1,
            _ => continue,
        }
        if count % 1000 == 0 {
            println!("{}", count);
        }
    }
    println!("{}", count);
    */
}

mod reader;
