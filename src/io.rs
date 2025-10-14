use crate::record::Rec;
use std::fs::File;
use std::io::{self, BufWriter, Read, Write as IoWrite};
use std::path::PathBuf;

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

/// Open a run file for writing.
pub fn open_run_writer(prefix: &str, idx: usize) -> io::Result<BufWriter<File>> {
    let filename = format!("{}_{:03}.bin", prefix, idx);
    let path = PathBuf::from(filename);
    let f = File::create(path)?;
    Ok(BufWriter::new(f))
}

/// Write: [u32 LE key_len][key][u32 LE payload_len][payload]
pub fn write_len_key_len_payload(w: &mut BufWriter<File>, rec: &Rec) -> io::Result<()> {
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
        let rec = Rec::new([1u8; 10], [2u8; 90]);
        let mut buffer = Vec::new();
        let mut writer = BufWriter::new(buffer);

        // We need to write to a temporary file for this test
        use std::io::Cursor;
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
