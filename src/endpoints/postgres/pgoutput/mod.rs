//! pgoutput logical decoding protocol — message types, wire decoder,
//! relation registry, and text-value → JSON mapping.
//!
//! Ported from faucet-stream (crates/source/postgres-cdc/src/pgoutput,
//! © the faucet-stream authors, dual-licensed Apache-2.0 OR MIT) and adapted
//! to mq-bridge. See <https://github.com/PawanSikawat/faucet-stream>.

pub mod decoder;
pub mod messages;
pub mod registry;
pub mod values;

use messages::{Relation, TupleCell, TupleData};
use serde_json::{Map, Value};

/// Flatten a decoded `TupleData` into a JSON object keyed by the relation's
/// column names — the `cdc_unwrap`-equivalent flat-row shape. `NULL` cells map
/// to JSON `null`; unchanged-TOAST cells are **omitted** (their value is not in
/// the WAL, so a downstream consumer can tell "unchanged" from "set to null").
/// Text cells are decoded to JSON by their column type OID via
/// [`values::text_to_json`], falling back to a JSON string on any decode error.
pub fn tuple_to_json_object(rel: &Relation, tuple: &TupleData) -> Map<String, Value> {
    let mut obj = Map::with_capacity(tuple.cells.len());
    for (col, cell) in rel.columns.iter().zip(tuple.cells.iter()) {
        match cell {
            TupleCell::Null => {
                obj.insert(col.name.clone(), Value::Null);
            }
            TupleCell::UnchangedToast => {
                // Omit — the value is not present in this change record.
            }
            TupleCell::Text(text) => {
                let value = values::text_to_json(col.type_oid, text)
                    .unwrap_or_else(|_| Value::String(text.clone()));
                obj.insert(col.name.clone(), value);
            }
        }
    }
    obj
}

#[cfg(test)]
mod tests {
    use super::messages::{ColumnDesc, ReplicaIdentity};
    use super::*;
    use serde_json::json;

    fn rel() -> Relation {
        Relation {
            oid: 16384,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                ColumnDesc {
                    flags: 1,
                    name: "id".into(),
                    type_oid: 23, // int4
                    type_modifier: -1,
                },
                ColumnDesc {
                    flags: 0,
                    name: "name".into(),
                    type_oid: 25, // text
                    type_modifier: -1,
                },
                ColumnDesc {
                    flags: 0,
                    name: "bio".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        }
    }

    #[test]
    fn flattens_typed_row() {
        let tuple = TupleData {
            cells: vec![
                TupleCell::Text("42".into()),
                TupleCell::Text("alice".into()),
                TupleCell::Null,
            ],
        };
        let obj = tuple_to_json_object(&rel(), &tuple);
        assert_eq!(
            Value::Object(obj),
            json!({"id": 42, "name": "alice", "bio": null})
        );
    }

    #[test]
    fn omits_unchanged_toast() {
        let tuple = TupleData {
            cells: vec![
                TupleCell::Text("42".into()),
                TupleCell::Text("alice".into()),
                TupleCell::UnchangedToast,
            ],
        };
        let obj = tuple_to_json_object(&rel(), &tuple);
        // `bio` is absent (not null) because its value was not in the WAL.
        assert_eq!(Value::Object(obj), json!({"id": 42, "name": "alice"}));
    }
}
