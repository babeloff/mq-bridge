//! Source-position ranges used by idempotent sinks.

use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};

use crate::CanonicalMessage;

const TOPIC_KEY: &str = "mqb.src.kafka_topic";
const PARTITION_KEY: &str = "mqb.src.kafka_partition";
const OFFSET_KEY: &str = "mqb.src.kafka_offset";
const POSTGRES_SLOT_KEY: &str = "mqb.src.postgres_slot";
const POSTGRES_LSN_KEY: &str = "mqb.src.postgres_lsn";
const POSTGRES_ORDINAL_KEY: &str = "mqb.src.postgres_ordinal";

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SourcePartition {
    pub topic: String,
    pub partition: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourcePosition {
    pub source: SourcePartition,
    pub offset: u64,
}

#[derive(Debug)]
pub struct SourceRun {
    pub source: SourcePartition,
    pub start: u64,
    pub end: u64,
    pub messages: Vec<CanonicalMessage>,
}

impl SourcePosition {
    pub fn from_message(message: &CanonicalMessage) -> Result<Self> {
        if message.metadata.contains_key(TOPIC_KEY)
            || message.metadata.contains_key(PARTITION_KEY)
            || message.metadata.contains_key(OFFSET_KEY)
        {
            return Self::from_kafka_message(message);
        }
        if message.metadata.contains_key(POSTGRES_SLOT_KEY)
            || message.metadata.contains_key(POSTGRES_LSN_KEY)
            || message.metadata.contains_key(POSTGRES_ORDINAL_KEY)
        {
            return Self::from_postgres_message(message);
        }
        bail!("idempotent sink requires Kafka or postgres_cdc source-position metadata");
    }

    fn from_kafka_message(message: &CanonicalMessage) -> Result<Self> {
        let topic = message
            .metadata
            .get(TOPIC_KEY)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("idempotent sink requires {TOPIC_KEY}"))?
            .clone();
        let partition = parse_signed_position(message, PARTITION_KEY)?;
        let offset = parse_unsigned_position(message, OFFSET_KEY)?;
        if partition < 0 {
            bail!("idempotent sink requires a non-negative Kafka partition");
        }
        let partition = i32::try_from(partition)
            .map_err(|_| anyhow!("idempotent sink Kafka partition is out of range"))?;

        Ok(Self {
            source: SourcePartition { topic, partition },
            offset,
        })
    }

    fn from_postgres_message(message: &CanonicalMessage) -> Result<Self> {
        let slot = message
            .metadata
            .get(POSTGRES_SLOT_KEY)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("idempotent sink requires {POSTGRES_SLOT_KEY}"))?;
        let lsn = parse_unsigned_position(message, POSTGRES_LSN_KEY)?;
        let ordinal = parse_unsigned_position(message, POSTGRES_ORDINAL_KEY)?;

        Ok(Self {
            // A commit LSN is shared by all changes in the transaction. Including it
            // in the source key makes the transaction ordinal a unique range offset.
            source: SourcePartition {
                topic: format!("postgres_cdc-{slot}-{lsn}"),
                partition: 0,
            },
            offset: ordinal,
        })
    }
}

fn parse_signed_position(message: &CanonicalMessage, key: &str) -> Result<i64> {
    message
        .metadata
        .get(key)
        .ok_or_else(|| anyhow!("idempotent sink requires {key}"))?
        .parse()
        .map_err(|_| anyhow!("idempotent sink requires an integer {key}"))
}

fn parse_unsigned_position(message: &CanonicalMessage, key: &str) -> Result<u64> {
    message
        .metadata
        .get(key)
        .ok_or_else(|| anyhow!("idempotent sink requires {key}"))?
        .parse()
        .map_err(|_| anyhow!("idempotent sink requires a non-negative integer {key}"))
}

#[derive(Clone, Debug, Default)]
pub struct CoveredRanges {
    ranges: BTreeMap<SourcePartition, Vec<(u64, u64)>>,
}

impl CoveredRanges {
    pub fn from_finalized_names<'a>(
        names: impl IntoIterator<Item = &'a str>,
        extension: &str,
    ) -> Self {
        let mut covered = Self::default();
        for name in names {
            if let Some((source, start, end)) = parse_finalized_name(name, extension) {
                // `parse_finalized_name` already guarantees a valid range.
                covered
                    .insert(source, start, end)
                    .expect("validated finalized range");
            }
        }
        covered
    }

    pub fn insert(&mut self, source: SourcePartition, start: u64, end: u64) -> Result<()> {
        if end < start {
            bail!("invalid covered source-position range {start}..={end}");
        }

        let ranges = self.ranges.entry(source).or_default();
        ranges.push((start, end));
        ranges.sort_unstable();

        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
        for (start, end) in ranges.drain(..) {
            if let Some((_, previous_end)) = merged.last_mut() {
                if start <= previous_end.saturating_add(1) {
                    *previous_end = (*previous_end).max(end);
                    continue;
                }
            }
            merged.push((start, end));
        }
        *ranges = merged;
        Ok(())
    }

    pub fn contains(&self, position: &SourcePosition) -> bool {
        self.ranges.get(&position.source).is_some_and(|ranges| {
            ranges
                .iter()
                .any(|(start, end)| *start <= position.offset && position.offset <= *end)
        })
    }

    pub fn filter_uncovered(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<Vec<CanonicalMessage>> {
        messages
            .into_iter()
            .filter_map(|message| match SourcePosition::from_message(&message) {
                Ok(position) if self.contains(&position) => None,
                Ok(_) => Some(Ok(message)),
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    pub fn uncovered_runs(&self, messages: Vec<CanonicalMessage>) -> Result<Vec<SourceRun>> {
        let mut grouped = BTreeMap::<SourcePartition, BTreeMap<u64, CanonicalMessage>>::new();
        for message in messages {
            let position = SourcePosition::from_message(&message)?;
            if !self.contains(&position) {
                grouped
                    .entry(position.source)
                    .or_default()
                    .entry(position.offset)
                    .or_insert(message);
            }
        }

        let mut runs = Vec::new();
        for (source, offsets) in grouped {
            let mut current: Option<SourceRun> = None;
            for (offset, message) in offsets {
                if let Some(run) = current.as_mut() {
                    if offset == run.end.saturating_add(1) {
                        run.end = offset;
                        run.messages.push(message);
                        continue;
                    }
                }
                if let Some(run) = current.take() {
                    runs.push(run);
                }
                current = Some(SourceRun {
                    source: source.clone(),
                    start: offset,
                    end: offset,
                    messages: vec![message],
                });
            }
            if let Some(run) = current {
                runs.push(run);
            }
        }
        Ok(runs)
    }
}

pub fn finalized_name(
    source: &SourcePartition,
    start: u64,
    end: u64,
    extension: &str,
) -> Result<String> {
    if source.topic.is_empty()
        || source.topic.contains('/')
        || source.partition < 0
        || extension.is_empty()
        || extension.contains('/')
        || end < start
    {
        bail!("invalid idempotent sink finalized name components");
    }
    Ok(format!(
        "part-{}-{}-{start}-{end}.{extension}",
        source.topic, source.partition
    ))
}

pub fn parse_finalized_name(name: &str, extension: &str) -> Option<(SourcePartition, u64, u64)> {
    let suffix = format!(".{extension}");
    let stem = name.strip_prefix("part-")?.strip_suffix(&suffix)?;
    let mut parts = stem.rsplitn(4, '-');
    let end = parts.next()?.parse().ok()?;
    let start = parts.next()?.parse().ok()?;
    let partition = parts.next()?.parse().ok()?;
    let topic = parts.next()?.to_string();
    if topic.is_empty() || partition < 0 || end < start {
        return None;
    }
    Some((SourcePartition { topic, partition }, start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(topic: &str, partition: i32, offset: u64) -> CanonicalMessage {
        let mut message = CanonicalMessage::new(b"payload".to_vec(), None);
        message.metadata.insert(TOPIC_KEY.into(), topic.into());
        message
            .metadata
            .insert(PARTITION_KEY.into(), partition.to_string());
        message
            .metadata
            .insert(OFFSET_KEY.into(), offset.to_string());
        message
    }

    fn postgres_message(slot: &str, lsn: u64, ordinal: u64) -> CanonicalMessage {
        let mut message = CanonicalMessage::new(b"payload".to_vec(), None);
        message
            .metadata
            .insert(POSTGRES_SLOT_KEY.into(), slot.into());
        message
            .metadata
            .insert(POSTGRES_LSN_KEY.into(), lsn.to_string());
        message
            .metadata
            .insert(POSTGRES_ORDINAL_KEY.into(), ordinal.to_string());
        message
    }

    #[test]
    fn source_position_requires_valid_kafka_metadata() {
        let error = SourcePosition::from_message(&CanonicalMessage::new(b"payload".to_vec(), None))
            .unwrap_err();
        assert!(error.to_string().contains("source-position metadata"));

        let mut invalid = message("orders", 0, 1);
        invalid
            .metadata
            .insert(OFFSET_KEY.into(), "not-an-offset".into());
        assert!(SourcePosition::from_message(&invalid).is_err());
    }

    #[test]
    fn postgres_cdc_positions_preserve_each_change_in_a_commit() {
        let runs = CoveredRanges::default()
            .uncovered_runs(vec![
                postgres_message("bridge_slot", 9_876_543_210, 0),
                postgres_message("bridge_slot", 9_876_543_210, 1),
                postgres_message("bridge_slot", 9_876_543_211, 0),
            ])
            .unwrap();

        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].source.topic, "postgres_cdc-bridge_slot-9876543210");
        assert_eq!(
            (runs[0].start, runs[0].end, runs[0].messages.len()),
            (0, 1, 2)
        );
        assert_eq!(runs[1].source.topic, "postgres_cdc-bridge_slot-9876543211");
        assert_eq!(
            (runs[1].start, runs[1].end, runs[1].messages.len()),
            (0, 0, 1)
        );
    }

    #[test]
    fn covered_ranges_merge_and_filter_each_record() {
        let source = SourcePartition {
            topic: "orders".into(),
            partition: 1,
        };
        let mut covered = CoveredRanges::default();
        covered.insert(source.clone(), 10, 12).unwrap();
        covered.insert(source.clone(), 13, 15).unwrap();
        covered.insert(source, 4, 6).unwrap();

        let messages = vec![
            message("orders", 1, 4),
            message("orders", 1, 7),
            message("orders", 1, 11),
            message("orders", 1, 16),
            message("orders", 2, 11),
        ];
        let uncovered = covered.filter_uncovered(messages).unwrap();
        let offsets = uncovered
            .iter()
            .map(|message| SourcePosition::from_message(message).unwrap())
            .map(|position| (position.source.partition, position.offset))
            .collect::<Vec<_>>();

        assert_eq!(offsets, vec![(1, 7), (1, 16), (2, 11)]);
    }

    #[test]
    fn finalized_names_round_trip_and_reject_staging_or_malformed_names() {
        let source = SourcePartition {
            topic: "orders-eu".into(),
            partition: 2,
        };
        let name = finalized_name(&source, 41, 59, "jsonl").unwrap();
        assert_eq!(name, "part-orders-eu-2-41-59.jsonl");
        assert_eq!(parse_finalized_name(&name, "jsonl"), Some((source, 41, 59)));
        assert_eq!(
            parse_finalized_name(".stage-part-orders-2-41-59.jsonl", "jsonl"),
            None
        );
        assert_eq!(
            parse_finalized_name("part-orders-2-59-41.jsonl", "jsonl"),
            None
        );
        assert_eq!(
            parse_finalized_name("part-orders-2-41-59.tmp", "jsonl"),
            None
        );
    }

    #[test]
    fn startup_recovery_uses_finalized_ranges_but_ignores_staging() {
        let covered = CoveredRanges::from_finalized_names(
            [
                "part-orders-0-10-12.jsonl",
                ".stage-orders-0-13-15.jsonl",
                "part-orders-0-14-15.jsonl",
                "part-orders-1-0-2.jsonl",
                "readme.jsonl",
            ],
            "jsonl",
        );

        let uncovered = covered
            .filter_uncovered(vec![
                message("orders", 0, 11),
                message("orders", 0, 13),
                message("orders", 0, 14),
                message("orders", 1, 1),
            ])
            .unwrap();
        let offsets = uncovered
            .iter()
            .map(|message| SourcePosition::from_message(message).unwrap().offset)
            .collect::<Vec<_>>();

        assert_eq!(offsets, vec![13]);
    }

    #[test]
    fn uncovered_runs_split_gaps_and_coalesce_duplicate_offsets() {
        let mut covered = CoveredRanges::default();
        covered
            .insert(
                SourcePartition {
                    topic: "orders".into(),
                    partition: 0,
                },
                2,
                2,
            )
            .unwrap();

        let runs = covered
            .uncovered_runs(vec![
                message("orders", 0, 3),
                message("orders", 0, 1),
                message("orders", 0, 3),
                message("orders", 1, 0),
                message("orders", 0, 5),
            ])
            .unwrap();

        let ranges = runs
            .iter()
            .map(|run| (run.source.partition, run.start, run.end, run.messages.len()))
            .collect::<Vec<_>>();
        assert_eq!(
            ranges,
            vec![(0, 1, 1, 1), (0, 3, 3, 1), (0, 5, 5, 1), (1, 0, 0, 1)]
        );
    }
}
