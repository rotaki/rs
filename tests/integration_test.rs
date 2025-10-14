use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;

// Helper to create a gensort-format file with specified keys
fn create_test_file(path: &str, keys: &[u8]) -> std::io::Result<()> {
    let mut file = File::create(path)?;
    for &key_byte in keys {
        let mut key = [0u8; 10];
        key[0] = key_byte;
        let payload = [0u8; 90];
        file.write_all(&key)?;
        file.write_all(&payload)?;
    }
    Ok(())
}

// Helper to read keys from a run file (with length prefixes)
fn read_run_file_keys(path: &str) -> std::io::Result<Vec<u8>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut keys = Vec::new();

    loop {
        // Read key length (4 bytes)
        let mut key_len_buf = [0u8; 4];
        match reader.read_exact(&mut key_len_buf) {
            Ok(_) => {
                let key_len = u32::from_le_bytes(key_len_buf) as usize;
                assert_eq!(key_len, 10, "Expected key length of 10");

                // Read key
                let mut key = vec![0u8; key_len];
                reader.read_exact(&mut key)?;
                keys.push(key[0]); // Store first byte as identifier

                // Read payload length (4 bytes)
                let mut payload_len_buf = [0u8; 4];
                reader.read_exact(&mut payload_len_buf)?;
                let payload_len = u32::from_le_bytes(payload_len_buf) as usize;
                assert_eq!(payload_len, 90, "Expected payload length of 90");

                // Read payload
                let mut payload = vec![0u8; payload_len];
                reader.read_exact(&mut payload)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
    }

    Ok(keys)
}

// Helper to verify a run is sorted
fn verify_run_sorted(keys: &[u8]) -> bool {
    keys.windows(2).all(|w| w[0] <= w[1])
}

// Helper to cleanup test files
fn cleanup_files(patterns: &[&str]) {
    for pattern in patterns {
        if let Some(idx) = pattern.find('*') {
            let prefix = &pattern[..idx];
            let suffix = &pattern[idx + 1..];
            for i in 0..100 {
                let filename = format!("{}{:03}{}", prefix, i, suffix);
                std::fs::remove_file(&filename).ok();
            }
        } else {
            std::fs::remove_file(pattern).ok();
        }
    }
}

#[test]
fn test_sorted_input_produces_single_run() {
    let input_file = "test_sorted_input.bin";
    let run_prefix = "test_sorted_run";

    // Create sorted input: 1, 2, 3, 4, 5
    create_test_file(input_file, &[1, 2, 3, 4, 5]).unwrap();

    // Run replacement selection with small heap
    let rs = rs::replacement_selection::ReplacementSelection::new(3, run_prefix.to_string());
    let num_runs = rs.run_from_file(input_file).unwrap();

    // Should produce exactly 1 run for sorted input
    assert_eq!(num_runs, 1, "Sorted input should produce single run");

    // Verify the run is sorted
    let keys = read_run_file_keys(&format!("{}_000.bin", run_prefix)).unwrap();
    assert!(verify_run_sorted(&keys), "Run should be sorted");
    assert_eq!(keys, vec![1, 2, 3, 4, 5], "All keys should be present");

    // Cleanup
    cleanup_files(&[input_file, &format!("{}_*.bin", run_prefix)]);
}

#[test]
fn test_reverse_sorted_produces_multiple_runs() {
    let input_file = "test_reverse_input.bin";
    let run_prefix = "test_reverse_run";

    // Create reverse sorted input: 5, 4, 3, 2, 1
    create_test_file(input_file, &[5, 4, 3, 2, 1]).unwrap();

    // Run replacement selection with heap capacity of 2
    let rs = rs::replacement_selection::ReplacementSelection::new(2, run_prefix.to_string());
    let num_runs = rs.run_from_file(input_file).unwrap();

    // Reverse sorted should produce multiple runs
    assert!(num_runs > 1, "Reverse sorted should produce multiple runs");

    // Verify each run is sorted
    for i in 0..num_runs {
        let run_file = format!("{}_{:03}.bin", run_prefix, i);
        let keys = read_run_file_keys(&run_file).unwrap();
        assert!(verify_run_sorted(&keys), "Run {} should be sorted", i);
    }

    // Cleanup
    cleanup_files(&[input_file, &format!("{}_*.bin", run_prefix)]);
}

#[test]
fn test_random_input_all_records_present() {
    let input_file = "test_random_input.bin";
    let run_prefix = "test_random_run";

    // Create random input
    let input_keys = vec![3, 1, 4, 1, 5, 9, 2, 6, 5, 3];
    create_test_file(input_file, &input_keys).unwrap();

    // Run replacement selection
    let rs = rs::replacement_selection::ReplacementSelection::new(4, run_prefix.to_string());
    let num_runs = rs.run_from_file(input_file).unwrap();

    // Collect all output keys from all runs
    let mut all_output_keys = Vec::new();
    for i in 0..num_runs {
        let run_file = format!("{}_{:03}.bin", run_prefix, i);
        if Path::new(&run_file).exists() {
            let mut keys = read_run_file_keys(&run_file).unwrap();
            all_output_keys.append(&mut keys);
        }
    }

    // Verify all input keys are in output
    assert_eq!(
        all_output_keys.len(),
        input_keys.len(),
        "All records should be present"
    );

    let mut sorted_input = input_keys.clone();
    sorted_input.sort();
    let mut sorted_output = all_output_keys.clone();
    sorted_output.sort();
    assert_eq!(
        sorted_output, sorted_input,
        "Output should contain same keys as input"
    );

    // Verify each run is individually sorted
    for i in 0..num_runs {
        let run_file = format!("{}_{:03}.bin", run_prefix, i);
        let keys = read_run_file_keys(&run_file).unwrap();
        assert!(verify_run_sorted(&keys), "Run {} should be sorted", i);
    }

    // Cleanup
    cleanup_files(&[input_file, &format!("{}_*.bin", run_prefix)]);
}

#[test]
fn test_empty_input() {
    let input_file = "test_empty_input.bin";
    let run_prefix = "test_empty_run";

    // Create empty file
    File::create(input_file).unwrap();

    // Run replacement selection
    let rs = rs::replacement_selection::ReplacementSelection::new(10, run_prefix.to_string());
    let num_runs = rs.run_from_file(input_file).unwrap();

    // Should produce 0 runs for empty input
    assert_eq!(num_runs, 0, "Empty input should produce 0 runs");

    // Cleanup
    cleanup_files(&[input_file, &format!("{}_*.bin", run_prefix)]);
}

#[test]
fn test_single_record() {
    let input_file = "test_single_input.bin";
    let run_prefix = "test_single_run";

    // Create file with single record
    create_test_file(input_file, &[42]).unwrap();

    // Run replacement selection
    let rs = rs::replacement_selection::ReplacementSelection::new(10, run_prefix.to_string());
    let num_runs = rs.run_from_file(input_file).unwrap();

    // Should produce 1 run
    assert_eq!(num_runs, 1, "Single record should produce 1 run");

    let keys = read_run_file_keys(&format!("{}_000.bin", run_prefix)).unwrap();
    assert_eq!(keys, vec![42], "Should contain the single key");

    // Cleanup
    cleanup_files(&[input_file, &format!("{}_*.bin", run_prefix)]);
}

#[test]
fn test_heap_capacity_effect() {
    let input_file = "test_capacity_input.bin";

    // Create reverse sorted input
    let input_keys: Vec<u8> = (1..=10).rev().collect();
    create_test_file(input_file, &input_keys).unwrap();

    // Test with small heap capacity
    let rs_small =
        rs::replacement_selection::ReplacementSelection::new(2, "test_cap_small".to_string());
    let num_runs_small = rs_small.run_from_file(input_file).unwrap();

    // Test with larger heap capacity
    let rs_large =
        rs::replacement_selection::ReplacementSelection::new(5, "test_cap_large".to_string());
    let num_runs_large = rs_large.run_from_file(input_file).unwrap();

    // Larger heap should produce fewer or equal runs
    assert!(
        num_runs_large <= num_runs_small,
        "Larger heap should not produce more runs than smaller heap"
    );

    // Cleanup
    cleanup_files(&[input_file, "test_cap_small_*.bin", "test_cap_large_*.bin"]);
}
