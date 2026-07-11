//! In-process cache of `Relation` messages so subsequent `Insert`/`Update`/
//! `Delete` events can look up column names + type OIDs by relation OID.
//!
//! Ported from faucet-stream (crates/source/postgres-cdc/src/pgoutput,
//! © the faucet-stream authors, dual-licensed Apache-2.0 OR MIT) and adapted
//! to mq-bridge's `anyhow` error type.

use super::messages::Relation;
use anyhow::anyhow;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct RelationRegistry {
    by_oid: HashMap<u32, Relation>,
}

impl RelationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, rel: Relation) {
        // A re-sent Relation with a different column set means the table's
        // schema changed mid-stream (ALTER TABLE). Subsequent tuples decode
        // against the *new* descriptor, but a same-arity rename/type change
        // can silently bind values to the wrong column names — surface it so
        // an operator can correlate any downstream surprise.
        if let Some(prev) = self.by_oid.get(&rel.oid) {
            let prev_cols: Vec<(&str, u32)> = prev
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.type_oid))
                .collect();
            let new_cols: Vec<(&str, u32)> = rel
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.type_oid))
                .collect();
            if prev_cols != new_cols {
                tracing::warn!(
                    relation = %rel.name,
                    oid = rel.oid,
                    "postgres-cdc: relation column set changed mid-stream (schema change); \
                     subsequent rows decode against the new descriptor"
                );
            }
        }
        self.by_oid.insert(rel.oid, rel);
    }

    pub fn get(&self, oid: u32) -> anyhow::Result<&Relation> {
        self.by_oid.get(&oid).ok_or_else(|| {
            anyhow!(
                "pgoutput: change event for unknown relation oid {oid} \
                 (Relation message must precede first change)"
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::messages::ReplicaIdentity;
    use super::*;

    fn rel(oid: u32, name: &str) -> Relation {
        Relation {
            oid,
            namespace: "public".into(),
            name: name.into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![],
        }
    }

    #[test]
    fn insert_then_get() {
        let mut r = RelationRegistry::new();
        r.insert(rel(16384, "users"));
        assert_eq!(r.get(16384).unwrap().name, "users");
    }

    #[test]
    fn second_insert_replaces() {
        let mut r = RelationRegistry::new();
        r.insert(rel(16384, "users_v1"));
        r.insert(rel(16384, "users_v2"));
        assert_eq!(r.get(16384).unwrap().name, "users_v2");
    }

    #[test]
    fn missing_oid_errors() {
        let r = RelationRegistry::new();
        let err = r.get(99999).unwrap_err();
        assert!(format!("{err}").contains("99999"));
    }

    #[test]
    fn reinsert_with_changed_columns_warns_and_replaces() {
        use super::super::messages::ColumnDesc;
        let col = |name: &str, oid: u32| ColumnDesc {
            flags: 0,
            name: name.into(),
            type_oid: oid,
            type_modifier: -1,
        };
        let mut rel_v1 = rel(16384, "users");
        rel_v1.columns = vec![col("id", 23)];
        let mut rel_v2 = rel(16384, "users");
        // A different column set (added column) exercises the schema-change
        // warning branch; subsequent lookups bind against the new descriptor.
        rel_v2.columns = vec![col("id", 23), col("email", 25)];

        let mut r = RelationRegistry::new();
        r.insert(rel_v1);
        r.insert(rel_v2);

        let got = r.get(16384).unwrap();
        assert_eq!(got.columns.len(), 2);
        assert_eq!(got.columns[1].name, "email");
        assert_eq!(got.columns[1].type_oid, 25);
    }
}
