use std::io::{Read, Result};

#[derive(Debug)]
pub struct DoubleBufferReader<'a> {
    first: &'a [u8],
    second: &'a [u8],
    position: usize,
    //end in bytes, including the entire length of first and the bytes in second until first
    //<ns:Statistik>. Will be initialized when end of first is reached
    end: usize,
    finished: bool,
}

impl<'a> DoubleBufferReader<'a> {
    pub fn new(first: &'a [u8], second: &'a [u8]) -> Self {
        Self {
            first,
            second,
            position: 0,
            end: 0, //this will be set during reading when needed
            finished: false,
        }
    }
}

const XML_PRE: &[u8] = b"<root>";

// invariant: at EOF either first or second will be a slice over the buffer. the len() of both always indicates valid data to read
impl<'a> Read for DoubleBufferReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        //TODO: handle buf2 length = 0 -> last thread, find last </ns:Statistik>
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
        if self.end == 0 {
            let end = find_str_in_u8("<ns:Statistik>", self.second).unwrap();
            self.end = end + self.first.len();
        }

        let end_in_second = self.end - self.first.len();
        let pos_in_second = self.position - self.first.len();
        let len = end_in_second - pos_in_second;

        if len < buf.len() {
            let buf = &mut buf[..len];
            buf.copy_from_slice(&self.second[pos_in_second..end_in_second]);
            self.finished = true;
            //TODO: still have to send XML_POST
            return Ok(len);
        } else {
            let end = buf.len() + pos_in_second;
            buf.copy_from_slice(&self.second[pos_in_second..end]);
            self.position += buf.len();
            return Ok(buf.len());
        }
    }
}

fn find_str_in_u8(needle: &str, haystack: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle.as_bytes())
}

#[test]
fn test_find_str_in_u8() {
    let haystack = "Hello 34zhfu 3kf f34 ¶world";
    let needle = "3kf";
    assert_eq!(find_str_in_u8(&needle, haystack.as_bytes()), Some(13));
}