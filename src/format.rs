//! Small formatting helpers shared by the UI and `--dump`.

use chrono::{DateTime, TimeDelta, Utc};

pub fn short_sha(sha: &str) -> &str {
    sha.get(..8).unwrap_or(sha)
}

/// Compact duration: `42s`, `3m05s`, `2h10m`, `4d`.
pub fn human(duration: TimeDelta) -> String {
    let secs = duration.num_seconds().max(0);
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m{:02}s", secs / 60, secs % 60),
        3600..86400 => format!("{}h{:02}m", secs / 3600, secs % 3600 / 60),
        _ => format!("{}d", secs / 86400),
    }
}

pub fn ago(time: DateTime<Utc>, now: DateTime<Utc>) -> String {
    human(now - time)
}

/// How long something ran, or has been running so far.
pub fn elapsed(
    started: Option<DateTime<Utc>>,
    finished: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<String> {
    let started = started?;
    Some(human(finished.unwrap_or(now) - started))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_durations() {
        assert_eq!(human(TimeDelta::seconds(-5)), "0s");
        assert_eq!(human(TimeDelta::seconds(59)), "59s");
        assert_eq!(human(TimeDelta::seconds(185)), "3m05s");
        assert_eq!(human(TimeDelta::seconds(7800)), "2h10m");
        assert_eq!(human(TimeDelta::days(4)), "4d");
        assert_eq!(short_sha("abc"), "abc");
        assert_eq!(short_sha("0123456789"), "01234567");
    }
}
