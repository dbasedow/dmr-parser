use std::io::{Read, Result};

#[derive(Debug)]
pub struct DoubleBufferReader<'a> {
    first: &'a [u8],
    second: &'a [u8],
    position: usize,
    finished: bool,
}

impl<'a> DoubleBufferReader<'a> {
    pub fn new(first: &'a [u8], second: &'a [u8]) -> Self {
        Self {
            first,
            second,
            position: 0,
            finished: false,
        }
    }
}

const XML_PRE: &[u8] = b"<root>";
const XML_POST: &[u8] = b"</root>";

// invariant: at EOF either first or second will be a slice over the buffer. the len() of both always indicates valid data to read
impl<'a> Read for DoubleBufferReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.finished {
            return Ok(0);
        }
        if self.position == 0 {
            //seek to first <ns:Statistik>
            self.position = find_str_in_u8("<ns:Statistik>", self.first).unwrap();
            let buf = &mut buf[..XML_PRE.len()];
            buf.copy_from_slice(XML_PRE);
            return Ok(XML_PRE.len());
        }
        if self.position < self.first.len() {
            let mut len = buf.len();
            let left = self.first.len() - self.position;
            if left < len {
                len = left;
            }
            let start = self.position;
            let end = self.position + len;
            self.position += len;
            let buf = &mut buf[..len];
            buf.copy_from_slice(&self.first[start..end]);
            return Ok(len);
        }
        let end = find_str_in_u8("<ns:Statistik>", self.second).unwrap();
        if end < buf.len() {
            let buf = &mut buf[..end];
            buf.copy_from_slice(&self.second[..end]);
            self.finished = true;
            //TODO: still have to send XML_POST
            return Ok(end);
        } else {
            let end = buf.len();
            let buf = &mut buf[..end];
            buf.copy_from_slice(&self.second[..end]);
            return Ok(buf.len());
        }
        unimplemented!()
    }
}

fn find_str_in_u8(needle: &str, haystack: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle.as_bytes())
}

#[test]
fn test_find_str_in_u8() {
    println!("heys");
    let haystack = "Hello 34zhfu 3kf f34 ¶world";
    let needle = "3kf";
    assert_eq!(find_str_in_u8(&needle, haystack.as_bytes()), Some(13));
}