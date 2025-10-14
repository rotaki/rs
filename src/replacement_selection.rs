use crate::io::{open_run_writer, read_gensort_record, write_len_key_len_payload};
use crate::record::Item;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};

pub struct ReplacementSelection {
    heap_cap: usize,
    out_prefix: String,
}

impl ReplacementSelection {
    pub fn new(heap_cap: usize, out_prefix: String) -> Self {
        Self {
            heap_cap,
            out_prefix,
        }
    }

    /// Run the replacement selection algorithm on the input.
    /// Returns the number of runs created.
    pub fn run<R: Read>(&self, mut rdr: R) -> io::Result<usize> {
        let mut heap: BinaryHeap<Reverse<Item>> = BinaryHeap::new();
        let mut seq: u64 = 0;
        let mut current_gen: u64 = 0;
        let mut run_idx: usize = 0;
        let mut last_out_key: Option<[u8; 10]> = None;

        // Prime heap with up to heap_cap records
        while heap.len() < self.heap_cap {
            match read_gensort_record(&mut rdr)? {
                Some(rec) => {
                    heap.push(Reverse(Item::new(rec, 0, seq)));
                    seq += 1;
                }
                None => break,
            }
        }

        if heap.is_empty() {
            return Ok(0);
        }

        // Open first run writer
        let mut writer = open_run_writer(&self.out_prefix, run_idx)?;
        let mut records_in_current_run = 0;

        // Main loop
        loop {
            if heap.is_empty() {
                break;
            }

            // If the smallest item is not from current_gen, current run is done.
            if heap.peek().map(|x| x.0.g).unwrap() != current_gen {
                // Only rotate if we actually wrote something to current run
                if records_in_current_run > 0 {
                    writer.flush()?;
                    run_idx += 1;
                    current_gen += 1;
                    last_out_key = None;
                    writer = open_run_writer(&self.out_prefix, run_idx)?;
                    records_in_current_run = 0;
                } else {
                    // This shouldn't happen in normal operation, but handle it defensively
                    current_gen += 1;
                }
                continue;
            }

            // Pop next output record
            let Reverse(item) = heap.pop().unwrap();
            write_len_key_len_payload(&mut writer, &item.rec)?;
            records_in_current_run += 1;

            last_out_key = Some(item.rec.key);

            // Refill: try to read one more input record and decide its generation
            if let Some(next_rec) = read_gensort_record(&mut rdr)? {
                let target_gen = match last_out_key {
                    Some(last) if next_rec.key < last => current_gen + 1, // freeze to future run
                    _ => current_gen,
                };
                heap.push(Reverse(Item::new(next_rec, target_gen, seq)));
                seq += 1;
            }
            // else: EOF; keep draining heap; run rotation will happen naturally when
            // only future-gen items remain.
        }

        writer.flush()?;
        Ok(run_idx + 1)
    }

    /// Run replacement selection from a file path
    pub fn run_from_file(&self, input_path: &str) -> io::Result<usize> {
        let f = File::open(input_path)?;
        let rdr = BufReader::new(f);
        self.run(rdr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Rec;
    use std::io::Cursor;

    fn create_test_record(key_byte: u8) -> Rec {
        let mut key = [0u8; 10];
        key[0] = key_byte;
        Rec::new(key, [0u8; 90])
    }

    #[test]
    fn test_empty_input() {
        let input = Cursor::new(vec![]);
        let rs = ReplacementSelection::new(10, "test_run".to_string());
        let num_runs = rs.run(input).unwrap();
        assert_eq!(num_runs, 0);
    }

    #[test]
    fn test_single_record() {
        let rec = create_test_record(1);
        let mut input = vec![0u8; 100];
        input[0..10].copy_from_slice(&rec.key);
        input[10..100].copy_from_slice(&rec.payload);

        let cursor = Cursor::new(input);
        let rs = ReplacementSelection::new(10, "test_run".to_string());
        let num_runs = rs.run(cursor).unwrap();
        assert_eq!(num_runs, 1);

        // Clean up
        std::fs::remove_file("test_run_000.bin").ok();
    }

    #[test]
    fn test_sorted_input_single_run() {
        // Create 5 records in ascending order
        let mut input = Vec::new();
        for i in 1..=5 {
            let rec = create_test_record(i);
            input.extend_from_slice(&rec.key);
            input.extend_from_slice(&rec.payload);
        }

        let cursor = Cursor::new(input);
        let rs = ReplacementSelection::new(3, "test_sorted".to_string());
        let num_runs = rs.run(cursor).unwrap();

        // All sorted input should produce single run
        assert_eq!(num_runs, 1);

        // Clean up
        std::fs::remove_file("test_sorted_000.bin").ok();
    }

    #[test]
    fn test_reverse_sorted_multiple_runs() {
        // Create 5 records in descending order
        let mut input = Vec::new();
        for i in (1..=5).rev() {
            let rec = create_test_record(i);
            input.extend_from_slice(&rec.key);
            input.extend_from_slice(&rec.payload);
        }

        let cursor = Cursor::new(input);
        let rs = ReplacementSelection::new(3, "test_reverse".to_string());
        let num_runs = rs.run(cursor).unwrap();

        // Reverse sorted should produce multiple runs
        // With heap_cap=3, worst case for reverse sorted is more runs
        assert!(num_runs > 1);

        // Clean up
        for i in 0..num_runs {
            std::fs::remove_file(format!("test_reverse_{:03}.bin", i)).ok();
        }
    }

    #[test]
    fn test_heap_capacity_limits_initial_load() {
        // Create 10 records
        let mut input = Vec::new();
        for i in 1..=10 {
            let rec = create_test_record(i);
            input.extend_from_slice(&rec.key);
            input.extend_from_slice(&rec.payload);
        }

        let cursor = Cursor::new(input);
        let rs = ReplacementSelection::new(3, "test_cap".to_string());
        let num_runs = rs.run(cursor).unwrap();

        // Should successfully process all records
        assert!(num_runs >= 1);

        // Clean up
        for i in 0..num_runs {
            std::fs::remove_file(format!("test_cap_{:03}.bin", i)).ok();
        }
    }
}
