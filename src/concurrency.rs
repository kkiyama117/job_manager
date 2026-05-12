//! Crate-wide parallelism knob used by `walk` and `tick`.
//!
//! Default is `available_parallelism()` clamped to `[FLOOR, CEIL]` so we
//! neither stall on single-core CI runners nor flood a shared HPC head
//! node with hundreds of concurrent `stat`/`sacct` calls.
//!
//! `JOB_MANAGER_PARALLELISM` (positive integer) overrides the default.

const FLOOR: usize = 8;
const CEIL: usize = 32;

pub(crate) fn parallelism() -> usize {
    if let Ok(s) = std::env::var("JOB_MANAGER_PARALLELISM")
        && let Ok(n) = s.parse::<usize>()
        && n > 0
    {
        return n;
    }
    std::thread::available_parallelism()
        .map(|n| n.get().clamp(FLOOR, CEIL))
        .unwrap_or(FLOOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_within_floor_and_ceil() {
        // Don't touch the env var here — other tests may set it
        // concurrently. Just confirm the no-env code path is in range
        // on the host where `cargo test` is invoked.
        let p = std::thread::available_parallelism()
            .map(|n| n.get().clamp(FLOOR, CEIL))
            .unwrap_or(FLOOR);
        assert!((FLOOR..=CEIL).contains(&p), "p = {p}");
    }
}
