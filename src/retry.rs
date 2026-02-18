use rand::Rng;
use std::time::Duration;

/// Calculate retry delay using **equal-jitter** exponential backoff.
///
/// Formula:
///   temp  = min(max_delay, base_delay * 2^attempt)
///   sleep = temp/2 + random(0 .. temp/2)
///
/// This keeps the delay at least half of the exponential value while adding
/// enough jitter to spread concurrent retries.
pub fn calculate_backoff(
    attempt: i32,
    base_delay_secs: u64,
    max_delay_secs: u64,
) -> Duration {
    let exp_delay = base_delay_secs.saturating_mul(2u64.saturating_pow(attempt.max(0) as u32));
    let capped = exp_delay.min(max_delay_secs);
    let half = capped / 2;

    let jitter = if half > 0 {
        rand::thread_rng().gen_range(0..=half)
    } else {
        0
    };

    Duration::from_secs((half + jitter).max(1))
}

/// Convenience: compute a concrete `chrono::DateTime<Utc>` for the next retry.
pub fn next_retry_at(attempt: i32) -> chrono::DateTime<chrono::Utc> {
    let delay = calculate_backoff(attempt, 5, 300); // 5 s base, 5 min cap
    chrono::Utc::now()
        + chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::seconds(5))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_never_zero() {
        for attempt in 0..20 {
            let d = calculate_backoff(attempt, 5, 300);
            assert!(d.as_secs() >= 1, "attempt {attempt}: delay was 0");
        }
    }

    #[test]
    fn backoff_respects_cap() {
        for attempt in 0..30 {
            let d = calculate_backoff(attempt, 5, 300);
            assert!(
                d.as_secs() <= 300,
                "attempt {attempt}: {} exceeded cap",
                d.as_secs()
            );
        }
    }

    #[test]
    fn backoff_generally_increases() {
        // With jitter the individual samples can flip, so we compare averages.
        let samples = 200;
        let avg = |attempt: i32| -> f64 {
            (0..samples)
                .map(|_| calculate_backoff(attempt, 5, 300).as_secs_f64())
                .sum::<f64>()
                / samples as f64
        };

        let a0 = avg(0);
        let a2 = avg(2);
        let a4 = avg(4);
        assert!(a2 > a0, "avg(2)={a2} should be > avg(0)={a0}");
        assert!(a4 > a2, "avg(4)={a4} should be > avg(2)={a2}");
    }

    #[test]
    fn next_retry_at_is_in_the_future() {
        let t = next_retry_at(0);
        assert!(t > chrono::Utc::now());
    }
}
