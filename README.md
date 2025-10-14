# Replacement Selection Sort

A modular implementation of the replacement selection algorithm for external sorting of gensort-formatted data.

## Architecture

The codebase is organized into modular components:

### Modules

- **[src/record.rs](src/record.rs)** - Data structures for records and heap items
  - `Rec`: 100-byte gensort record (10-byte key + 90-byte payload)
  - `Item`: Heap item with generation tracking for run management
  - Ordering implementations for min-heap behavior

- **[src/io.rs](src/io.rs)** - I/O operations for reading and writing records
  - `read_exact_into()`: Read exactly N bytes with EOF handling
  - `read_gensort_record()`: Read gensort format records
  - `write_len_key_len_payload()`: Write records with length prefixes
  - `open_run_writer()`: Create run output files

- **[src/replacement_selection.rs](src/replacement_selection.rs)** - Core algorithm implementation
  - `ReplacementSelection`: Main algorithm struct
  - Handles heap management, generation tracking, and run rotation
  - Prevents empty run file creation

- **[src/main.rs](src/main.rs)** - CLI entry point
- **[src/lib.rs](src/lib.rs)** - Library interface for testing

## Usage

```bash
# Run with default settings (10M record heap = ~1GB memory)
cargo run --release -- <gensort_input.bin>

# Custom heap capacity (number of records)
HEAP_CAP=5000000 cargo run --release -- input.bin

# Custom output prefix
RUN_PREFIX=sorted cargo run --release -- input.bin
```

### Output

The program creates sorted run files:
- `run_000.bin`, `run_001.bin`, etc. (or custom prefix)
- Each run contains records sorted by key
- Output format: `[u32 key_len][key][u32 payload_len][payload]`

## Testing

The project includes comprehensive unit and integration tests:

```bash
# Run all tests
cargo test

# Run unit tests only
cargo test --lib

# Run specific module tests
cargo test --lib record::tests
cargo test --lib io::tests
cargo test --lib replacement_selection::tests

# Run integration tests
cargo test --test integration_test
```

### Test Coverage

- **Record module**: 4 tests covering ordering and creation
- **I/O module**: 8 tests for reading/writing operations
- **Replacement selection**: 5 unit tests + 6 integration tests
- Tests cover: empty input, sorted/reverse sorted data, edge cases, heap capacity limits

## Algorithm Details

### Replacement Selection

1. **Initial Load**: Fill heap with up to `heap_cap` records (all generation 0)
2. **Main Loop**:
   - Pop minimum record from heap and write to current run
   - Read next input record
   - If new record's key < last output key → freeze to next generation
   - Otherwise → add to current generation
3. **Run Rotation**: When heap contains only future-generation items, start new run
4. **Safety**: Prevents empty run files by tracking records written

### Key Fix

The refactored code includes a critical bug fix:
- Tracks `records_in_current_run` counter
- Only rotates to new run file if current run has data
- Prevents creation of empty run files in edge cases

## Performance

- Memory usage: `heap_cap * 100 bytes`
- Default: 10M records × 100 bytes = ~1GB
- Adjust `HEAP_CAP` based on available memory
- Larger heap → fewer runs → better merge phase performance

## File Format

### Input (gensort binary)
- 100 bytes per record
- 10-byte key + 90-byte payload
- No delimiters or headers

### Output (run files)
- Length-prefixed format for compatibility
- 4 bytes (u32 LE): key length (always 10)
- 10 bytes: key
- 4 bytes (u32 LE): payload length (always 90)
- 90 bytes: payload
- Total: 108 bytes per record

## Dependencies

None - uses only Rust standard library.

## License

This is a demonstration/educational implementation.
