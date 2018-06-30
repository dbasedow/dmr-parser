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

#[derive(Debug)]
struct CarInfo {
    id: String,
    typ: String,
    type_name: String,
    license_plate: String,
    vin: String,
    first_registration: String,
    brand: String,
    model: String,
    variant: String,
    model_year: String,
    registration_ended: String,
    status: String,
    status_date: String,

    //Keep track of where the xml parser is
    current_tag: CurrentTag,
}

impl CarInfo {
    fn new() -> Self {
        CarInfo {
            id: String::new(),
            typ: String::new(),
            type_name: String::new(),
            license_plate: String::new(),
            vin: String::new(),
            first_registration: String::new(),
            brand: String::new(),
            model: String::new(),
            variant: String::new(),
            model_year: String::new(),
            registration_ended: String::new(),
            status: String::new(),
            status_date: String::new(),
            current_tag: CurrentTag::None,
        }
    }

    fn handle_text(&mut self, v: String) {
        match self.current_tag {
            CurrentTag::None => return,
            CurrentTag::Id => self.id = v,
            CurrentTag::Type => self.typ = v,
            CurrentTag::TypeName => self.type_name = v,
            CurrentTag::LicensePlate => self.license_plate = v,
            CurrentTag::Vin => self.vin = v,
            CurrentTag::FirstRegistration => self.first_registration = v,
            CurrentTag::Brand => self.brand = v,
            CurrentTag::Model => self.model = v,
            CurrentTag::Variant => self.variant = v,
            CurrentTag::ModelYear => self.model_year = v,
            CurrentTag::RegistrationEnded => self.registration_ended = v,
            CurrentTag::Status => self.status = v,
            CurrentTag::StatusDate => self.status_date = v,
            _ => {}
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

pub fn parser_worker(b1: Arc<RwLock<Vec<u8>>>, b2: Arc<RwLock<Vec<u8>>>, logger: Sender<String>) {
    let b1 = b1.read().unwrap();
    let b2 = b2.read().unwrap();
    let dbr = DoubleBufferReader::new(&b1, &b2);
    let br = BufReader::new(dbr);

    let mut xml = Reader::from_reader(br);
    let mut count = 0;
    let mut buf = vec![0; 20000];
    let mut cur_car = CarInfo::new();

    loop {
        match xml.read_event(&mut buf) {
            Ok(Event::Start(ref tag)) => {
                match tag.name() {
                    b"ns:Statistik" => cur_car = CarInfo::new(),
                    b"ns:KoeretoejIdent" => cur_car.current_tag = CurrentTag::Id,
                    b"ns:KoeretoejArtNummer" => cur_car.current_tag = CurrentTag::Type,
                    b"ns:KoeretoejArtNavn" => cur_car.current_tag = CurrentTag::TypeName,
                    b"ns:RegistreringNummerNummer" => cur_car.current_tag = CurrentTag::LicensePlate,
                    b"ns:KoeretoejOplysningStelNummer" => cur_car.current_tag = CurrentTag::Vin,
                    b"ns:KoeretoejOplysningFoersteRegistreringDato" => cur_car.current_tag = CurrentTag::FirstRegistration,
                    b"ns:KoeretoejMaerkeTypeNavn" => cur_car.current_tag = CurrentTag::Brand,
                    b"ns:KoeretoejModelTypeNavn" => cur_car.current_tag = CurrentTag::Model,
                    b"ns:KoeretoejVariantTypeNavn" => cur_car.current_tag = CurrentTag::Variant,
                    b"ns:KoeretoejOplysningModelAar" => cur_car.current_tag = CurrentTag::ModelYear,
                    b"ns:RegistreringNummerUdloebDato" => cur_car.current_tag = CurrentTag::RegistrationEnded,
                    b"ns:KoeretoejRegistreringStatus" => cur_car.current_tag = CurrentTag::Status,
                    b"ns:KoeretoejRegistreringStatusDato" => cur_car.current_tag = CurrentTag::StatusDate,
                    _ => {}
                }
            }
            Ok(Event::End(ref tag)) => {
                match tag.name() {
                    b"ns:Statistik" => println!("{:?}", cur_car),
                    _ => cur_car.current_tag = CurrentTag::None,
                }
            }
            Ok(Event::Text(e)) => cur_car.handle_text(e.unescape_and_decode(&xml).unwrap()),
            Ok(Event::Eof) => break,
            Err(e) => println!("{:?}", e),
            _ => {}
        }
        buf.clear();
    }
    logger.send(format!("Statistik tags: {}", count));
}
