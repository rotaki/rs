mod io;
mod record;
pub mod replacement_selection;

use replacement_selection::ReplacementSelection;

fn main() -> std::io::Result<()> {
    // ---- CLI & params ----
    // Usage: cargo run --release -- <gensort_input.bin>
    let input_path = std::env::args()
        .nth(1)
        .expect("Usage: replacement_selection <gensort_input.bin>");
    let heap_cap = std::env::var("HEAP_CAP")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10_000_000); // adjust: memory_budget / record_size // 10M * 100bytes = 1GB
    let out_prefix = std::env::var("RUN_PREFIX").unwrap_or_else(|_| "run".to_string());

    // ---- Run replacement selection ----
    let rs = ReplacementSelection::new(heap_cap, out_prefix.clone());
    let num_runs = rs.run_from_file(&input_path)?;

    eprintln!("Wrote {} run(s) with prefix '{}_'", num_runs, out_prefix);
    Ok(())
}
