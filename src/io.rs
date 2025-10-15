use crate::record::Rec;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write as IoWrite};
use std::path::PathBuf;

#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

/// Direct I/O alignment requirement (typically 512 or 4096)
const ALIGNMENT: usize = 4096;

/// Helper to create aligned buffer
fn aligned_buffer(size: usize) -> Vec<u8> {
    let layout = std::alloc::Layout::from_size_align(size, ALIGNMENT).unwrap();
    let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
    if ptr.is_null() {
        panic!("Failed to allocate aligned buffer");
    }
    unsafe { Vec::from_raw_parts(ptr, size, size) }
}

/// Reader wrapper for Direct I/O with alignment handling
pub struct DirectReader {
    file: File,
    buffer: Vec<u8>,
    buffer_pos: usize,   // Current position in buffer
    buffer_valid: usize, // Valid data in buffer
    file_pos: u64,       // Current file position
    file_size: u64,      // Total file size
}

impl DirectReader {
    pub fn new(file: File) -> io::Result<Self> {
        // Get file size
        let file_size = file.metadata()?.len();

        Ok(Self {
            file,
            buffer: aligned_buffer(ALIGNMENT),
            buffer_pos: 0,
            buffer_valid: 0,
            file_pos: 0,
            file_size,
        })
    }

    /// Fill the buffer with the next aligned block from file
    fn fill_buffer(&mut self) -> io::Result<bool> {
        if self.file_pos >= self.file_size {
            return Ok(false); // EOF
        }

        // Read aligned block
        let bytes_read = self.file.read(&mut self.buffer)?;
        if bytes_read == 0 {
            return Ok(false); // EOF
        }

        // Calculate how much valid data we have (don't exceed file size)
        let remaining_in_file = (self.file_size - self.file_pos) as usize;
        self.buffer_valid = bytes_read.min(remaining_in_file);
        self.buffer_pos = 0;
        self.file_pos += bytes_read as u64;

        Ok(true)
    }
}

impl Read for DirectReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.buffer_pos >= self.buffer_valid {
            // Need to refill buffer
            if !self.fill_buffer()? {
                return Ok(0); // EOF
            }
        }

        // Copy from buffer to output
        let available = self.buffer_valid - self.buffer_pos;
        let to_copy = available.min(buf.len());
        buf[..to_copy].copy_from_slice(&self.buffer[self.buffer_pos..self.buffer_pos + to_copy]);
        self.buffer_pos += to_copy;

        Ok(to_copy)
    }
}

/// Open a file for reading with Direct I/O.
pub fn open_direct_reader(path: &str) -> io::Result<DirectReader> {
    #[cfg(target_os = "linux")]
    let f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?;

    #[cfg(not(target_os = "linux"))]
    let f = OpenOptions::new().read(true).open(path)?;

    DirectReader::new(f)
}

/// Read exactly N bytes into an array. Returns None on clean EOF, error on partial read.
pub fn read_exact_into<const N: usize>(r: &mut impl Read) -> io::Result<Option<[u8; N]>> {
    let mut buf = [0u8; N];
    let mut read = 0usize;
    while read < N {
        match r.read(&mut buf[read..])? {
            0 => {
                if read == 0 {
                    return Ok(None); // clean EOF before starting
                } else {
                    // partial record at EOF -> treat as error
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated record at EOF",
                    ));
                }
            }
            n => read += n,
        }
    }
    Ok(Some(buf))
}

/// Read one gensort record (10-byte key + 90-byte payload). None on clean EOF.
pub fn read_gensort_record(r: &mut impl Read) -> io::Result<Option<Rec>> {
    let key = match read_exact_into::<10>(r)? {
        Some(k) => k,
        None => return Ok(None),
    };
    let payload = match read_exact_into::<90>(r)? {
        Some(p) => p,
        None => {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "payload missing (truncated gensort record)",
            ));
        }
    };
    Ok(Some(Rec::new(key, payload)))
}

/// Writer wrapper for Direct I/O with alignment handling
pub struct DirectWriter {
    file: File,
    buffer: Vec<u8>,
    pos: usize,
    total_bytes_written: u64, // Track actual data size (not including padding)
}

impl DirectWriter {
    pub fn new(file: File) -> Self {
        Self {
            file,
            buffer: aligned_buffer(ALIGNMENT),
            pos: 0,
            total_bytes_written: 0,
        }
    }

    /// Write data to the buffer, flushing when full
    pub fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        let mut offset = 0;
        while offset < data.len() {
            let remaining = data.len() - offset;
            let space = ALIGNMENT - self.pos;

            if remaining >= space {
                // Fill current buffer and flush
                self.buffer[self.pos..ALIGNMENT].copy_from_slice(&data[offset..offset + space]);
                self.file.write_all(&self.buffer)?;
                self.total_bytes_written += space as u64;
                self.pos = 0;
                offset += space;
            } else {
                // Fits in current buffer
                self.buffer[self.pos..self.pos + remaining].copy_from_slice(&data[offset..]);
                self.pos += remaining;
                self.total_bytes_written += remaining as u64;
                offset += remaining;
            }
        }
        Ok(())
    }

    /// Flush remaining data (pad to alignment if needed), then truncate to actual size
    pub fn flush(&mut self) -> io::Result<()> {
        if self.pos > 0 {
            // Pad to alignment
            for i in self.pos..ALIGNMENT {
                self.buffer[i] = 0;
            }
            self.file.write_all(&self.buffer)?;
            self.pos = 0;
        }

        // Truncate file to actual data size (remove padding)
        // This requires the file to support seeking, which works with regular files
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            unsafe {
                libc::ftruncate(self.file.as_raw_fd(), self.total_bytes_written as i64);
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            self.file.set_len(self.total_bytes_written)?;
        }

        Ok(())
    }
}

impl Drop for DirectWriter {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

/// Open a run file for writing with Direct I/O.
pub fn open_run_writer(prefix: &str, idx: usize) -> io::Result<DirectWriter> {
    let filename = format!("{}_{:03}.bin", prefix, idx);
    let path = PathBuf::from(filename);

    #[cfg(target_os = "linux")]
    let f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?;

    #[cfg(not(target_os = "linux"))]
    let f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;

    Ok(DirectWriter::new(f))
}

/// Write: [u32 LE key_len][key][u32 LE payload_len][payload]
pub fn write_len_key_len_payload(w: &mut DirectWriter, rec: &Rec) -> io::Result<()> {
    let key_len_le = (10u32).to_le_bytes();
    let payload_len_le = (90u32).to_le_bytes();
    w.write_all(&key_len_le)?;
    w.write_all(&rec.key)?;
    w.write_all(&payload_len_le)?;
    w.write_all(&rec.payload)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_exact_into_success() {
        let data = [1u8, 2, 3, 4, 5];
        let mut cursor = Cursor::new(data);
        let result = read_exact_into::<5>(&mut cursor).unwrap();
        assert_eq!(result, Some([1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_read_exact_into_eof() {
        let data = [];
        let mut cursor = Cursor::new(data);
        let result = read_exact_into::<5>(&mut cursor).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_read_exact_into_partial() {
        let data = [1u8, 2, 3];
        let mut cursor = Cursor::new(data);
        let result = read_exact_into::<5>(&mut cursor);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_read_gensort_record_success() {
        let mut data = vec![0u8; 100];
        data[0..10].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        data[10..20].copy_from_slice(&[11, 12, 13, 14, 15, 16, 17, 18, 19, 20]);

        let mut cursor = Cursor::new(data);
        let result = read_gensort_record(&mut cursor).unwrap();
        assert!(result.is_some());

        let rec = result.unwrap();
        assert_eq!(&rec.key[..], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(
            &rec.payload[0..10],
            &[11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        );
    }

    #[test]
    fn test_read_gensort_record_eof() {
        let data = vec![];
        let mut cursor = Cursor::new(data);
        let result = read_gensort_record(&mut cursor).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_read_gensort_record_partial_key() {
        let data = vec![1u8, 2, 3, 4, 5]; // only 5 bytes, need 10 for key
        let mut cursor = Cursor::new(data);
        let result = read_gensort_record(&mut cursor);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_gensort_record_partial_payload() {
        let mut data = vec![0u8; 50]; // key + partial payload
        data[0..10].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        let mut cursor = Cursor::new(data);
        let result = read_gensort_record(&mut cursor);
        assert!(result.is_err());
        // Error is from read_exact_into for partial read
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("truncated") || err_msg.contains("payload missing"));
    }

    #[test]
    fn test_write_len_key_len_payload() {
        use std::io::Cursor;

        let rec = Rec::new([1u8; 10], [2u8; 90]);
        let mut output = Cursor::new(Vec::new());

        // Manually write what write_len_key_len_payload does
        output.write_all(&(10u32).to_le_bytes()).unwrap();
        output.write_all(&rec.key).unwrap();
        output.write_all(&(90u32).to_le_bytes()).unwrap();
        output.write_all(&rec.payload).unwrap();

        let result = output.into_inner();
        assert_eq!(result.len(), 4 + 10 + 4 + 90); // Total: 108 bytes

        // Verify structure
        let key_len = u32::from_le_bytes([result[0], result[1], result[2], result[3]]);
        assert_eq!(key_len, 10);

        let payload_len = u32::from_le_bytes([result[14], result[15], result[16], result[17]]);
        assert_eq!(payload_len, 90);
    }
}
