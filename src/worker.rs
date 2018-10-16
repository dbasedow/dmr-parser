use flate2::bufread::DeflateDecoder;
use quick_xml::events::{BytesText, Event};
use quick_xml::Reader;
use reader::DoubleBufferReader;
use serde_json;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Read};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;

#[derive(Debug, Serialize)]
struct CarInfo {
    id: Option<String>,
    typ: Option<String>,
    type_name: Option<String>,
    license_plate: Option<String>,
    vin: Option<String>,
    first_registration: Option<String>,
    brand: Option<String>,
    model: Option<String>,
    variant: Option<String>,
    model_year: Option<String>,
    registration_ended: Option<String>,
    status: Option<String>,
    status_date: Option<String>,

    //Keep track of where the xml parser is
    #[serde(skip_serializing)]
    current_tag: CurrentTag,
}

impl CarInfo {
    fn new() -> Self {
        CarInfo {
            id: None,
            typ: None,
            type_name: None,
            license_plate: None,
            vin: None,
            first_registration: None,
            brand: None,
            model: None,
            variant: None,
            model_year: None,
            registration_ended: None,
            status: None,
            status_date: None,
            current_tag: CurrentTag::None,
        }
    }

    fn handle_text<B: BufRead>(&mut self, e: &BytesText, xml: &Reader<B>) {
        match self.current_tag {
            CurrentTag::None => return,
            CurrentTag::Id => self.id = e.unescape_and_decode(&xml).ok(),
            CurrentTag::Type => self.typ = e.unescape_and_decode(&xml).ok(),
            CurrentTag::TypeName => self.type_name = e.unescape_and_decode(&xml).ok(),
            CurrentTag::LicensePlate => self.license_plate = e.unescape_and_decode(&xml).ok(),
            CurrentTag::Vin => self.vin = e.unescape_and_decode(&xml).ok(),
            CurrentTag::FirstRegistration => {
                self.first_registration = e.unescape_and_decode(&xml).ok()
            }
            CurrentTag::Brand => self.brand = e.unescape_and_decode(&xml).ok(),
            CurrentTag::Model => self.model = e.unescape_and_decode(&xml).ok(),
            CurrentTag::Variant => self.variant = e.unescape_and_decode(&xml).ok(),
            CurrentTag::ModelYear => self.model_year = e.unescape_and_decode(&xml).ok(),
            CurrentTag::RegistrationEnded => {
                self.registration_ended = e.unescape_and_decode(&xml).ok()
            }
            CurrentTag::Status => self.status = e.unescape_and_decode(&xml).ok(),
            CurrentTag::StatusDate => self.status_date = e.unescape_and_decode(&xml).ok(),
        }
    }
}

#[derive(Debug)]
enum CurrentTag {
    None,
    Id,
    Type,
    TypeName,
    LicensePlate,
    Vin,
    FirstRegistration,
    Brand,
    Model,
    Variant,
    ModelYear,
    RegistrationEnded,
    Status,
    StatusDate,
}

fn parser_worker(b1: Arc<RwLock<Vec<u8>>>, b2: Arc<RwLock<Vec<u8>>>, logger: Sender<String>) {
    let b1 = b1.read().unwrap();
    let b2 = b2.read().unwrap();
    let dbr = DoubleBufferReader::new(&b1, &b2);
    let br = BufReader::new(dbr);

    let mut xml = Reader::from_reader(br);
    xml.check_end_names(false);

    let mut buf = vec![0; 20000];
    let mut cur_car = CarInfo::new();

    loop {
        match xml.read_event(&mut buf) {
            Ok(Event::Start(ref tag)) => match tag.name() {
                b"ns:Statistik" => cur_car = CarInfo::new(),
                b"ns:KoeretoejIdent" => cur_car.current_tag = CurrentTag::Id,
                b"ns:KoeretoejArtNummer" => cur_car.current_tag = CurrentTag::Type,
                b"ns:KoeretoejArtNavn" => cur_car.current_tag = CurrentTag::TypeName,
                b"ns:RegistreringNummerNummer" => cur_car.current_tag = CurrentTag::LicensePlate,
                b"ns:KoeretoejOplysningStelNummer" => cur_car.current_tag = CurrentTag::Vin,
                b"ns:KoeretoejOplysningFoersteRegistreringDato" => {
                    cur_car.current_tag = CurrentTag::FirstRegistration
                }
                b"ns:KoeretoejMaerkeTypeNavn" => cur_car.current_tag = CurrentTag::Brand,
                b"ns:KoeretoejModelTypeNavn" => cur_car.current_tag = CurrentTag::Model,
                b"ns:KoeretoejVariantTypeNavn" => cur_car.current_tag = CurrentTag::Variant,
                b"ns:KoeretoejOplysningModelAar" => cur_car.current_tag = CurrentTag::ModelYear,
                b"ns:RegistreringNummerUdloebDato" => {
                    cur_car.current_tag = CurrentTag::RegistrationEnded
                }
                b"ns:KoeretoejRegistreringStatus" => cur_car.current_tag = CurrentTag::Status,
                b"ns:KoeretoejRegistreringStatusDato" => {
                    cur_car.current_tag = CurrentTag::StatusDate
                }
                _ => {}
            },
            Ok(Event::End(ref tag)) => match tag.name() {
                b"ns:Statistik" => {
                    if let Some(ref vin) = cur_car.vin {
                        if vin.len() == 17 {
                            let _ = logger.send(serde_json::to_string(&cur_car).unwrap());
                        }
                    }
                }
                _ => cur_car.current_tag = CurrentTag::None,
            },
            Ok(Event::Text(e)) => cur_car.handle_text(&e, &xml),
            Ok(Event::Eof) => break,
            Err(e) => eprintln!("{:?}", e),
            _ => {}
        }
        buf.clear();
    }
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

fn process_zip_header(f: &mut BufReader<File>) -> io::Result<()> {
    let mut header_buf = [0; 30];

    f.read_exact(&mut header_buf)
        .expect("unable to read header");

    assert_eq!(header_buf[..4], [0x50, 0x4b, 0x03, 0x04]);
    let name_len = ((header_buf[27] as usize) << 8) + header_buf[26] as usize;
    let extra_len = ((header_buf[29] as usize) << 8) + header_buf[28] as usize;

    let mut name_extra_buf = vec![0; name_len + extra_len];
    f.read_exact(&mut name_extra_buf)?;
    Ok(())
}

pub fn process_file(
    filename: &str,
    log_tx: Sender<String>,
    buffer_count: usize,
    buffer_size: usize,
) -> io::Result<()> {
    let f = File::open(filename).unwrap();
    let mut f = BufReader::new(f);

    process_zip_header(&mut f)?;

    let mut deflater = DeflateDecoder::new(f);

    let mut bufs = Vec::with_capacity(buffer_count);
    for _ in 0..buffer_count {
        bufs.push(Arc::new(RwLock::new(vec![0; buffer_size])));
    }

    //we need two buffers to start handing buffer pairs off to threads. create one now, so the
    //loop doesn't have to care
    {
        let mut b = bufs[0].write().unwrap();
        fill_buffer(&mut b, &mut deflater)?;
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
        let _ = bufs[i].write().unwrap();
    }
    Ok(())
}
