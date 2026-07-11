//! LSN <-> text helpers for replication-slot progress.
//!
//! The confirmed LSN is stored in the checkpoint store as Postgres' canonical
//! `XXXXXXXX/XXXXXXXX` text form (the opaque-string contract of
//! [`crate::checkpoint::CheckpointStore`]). It is the **`end_lsn`** of the last
//! committed transaction whose changes were durably acknowledged downstream —
//! exactly where the next `START_REPLICATION` resumes.
//!
//! `format_lsn` / `parse_lsn` are ported from faucet-stream
//! (crates/source/postgres-cdc/src/state.rs, dual Apache-2.0 OR MIT).

use anyhow::anyhow;

/// Format a `u64` LSN into Postgres' `XXXXXXXX/XXXXXXXX` text form, dropping
/// leading zeros from each half (matches `pg_lsn` output).
pub fn format_lsn(lsn: u64) -> String {
    let hi = (lsn >> 32) as u32;
    let lo = lsn as u32;
    format!("{hi:X}/{lo:X}")
}

/// Parse `XXXX/XXXX` (case-insensitive hex, no leading zeros required) into a
/// `u64`. Returns an error on any malformation.
pub fn parse_lsn(s: &str) -> anyhow::Result<u64> {
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| anyhow!("postgres-cdc invalid LSN '{s}': missing '/'"))?;
    if hi.is_empty() || lo.is_empty() || lo.contains('/') {
        return Err(anyhow!("postgres-cdc invalid LSN '{s}'"));
    }
    let hi = u32::from_str_radix(hi, 16)
        .map_err(|e| anyhow!("postgres-cdc LSN high half '{hi}': {e}"))?;
    let lo = u32::from_str_radix(lo, 16)
        .map_err(|e| anyhow!("postgres-cdc LSN low half '{lo}': {e}"))?;
    Ok((u64::from(hi) << 32) | u64::from(lo))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_text_round_trip() {
        assert_eq!(parse_lsn(&format_lsn(0)).unwrap(), 0);
        assert_eq!(parse_lsn(&format_lsn(u64::MAX)).unwrap(), u64::MAX);
        let v: u64 = 0x1234_5678_9ABC_DEF0;
        assert_eq!(parse_lsn(&format_lsn(v)).unwrap(), v);
        assert_eq!(format_lsn(0x1A_BEEF_CAFE), "1A/BEEFCAFE");
    }

    #[test]
    fn parse_lsn_is_case_insensitive() {
        assert_eq!(
            parse_lsn("0/16a4f88").unwrap(),
            parse_lsn("0/16A4F88").unwrap()
        );
    }

    #[test]
    fn parse_lsn_rejects_malformed() {
        assert!(parse_lsn("").is_err());
        assert!(parse_lsn("not-an-lsn").is_err());
        assert!(parse_lsn("0/").is_err());
        assert!(parse_lsn("/0").is_err());
        assert!(parse_lsn("0/16A4F88/extra").is_err());
    }
}
