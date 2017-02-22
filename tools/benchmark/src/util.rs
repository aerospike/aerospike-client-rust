use std::time::Instant;

pub fn elapsed_millis(time: Instant) -> u64 {
    let elapsed = time.elapsed();
    let secs = elapsed.as_secs();
    let nanos = elapsed.subsec_nanos() as u64;
    secs * 1_000 + nanos / 1_000_000
}
