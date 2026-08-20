/// Top-level object fields borrowed from a JSON payload, with each value left as raw JSON.
pub(crate) struct RawPairs<'a>(pub(crate) Vec<(&'a str, &'a serde_json::value::RawValue)>);

impl<'de> serde::Deserialize<'de> for RawPairs<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct PairVisitor;

        impl<'de> serde::de::Visitor<'de> for PairVisitor {
            type Value = RawPairs<'de>;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<RawPairs<'de>, A::Error> {
                let mut pairs = Vec::with_capacity(map.size_hint().unwrap_or(8));
                while let Some(entry) =
                    map.next_entry::<&'de str, &'de serde_json::value::RawValue>()?
                {
                    pairs.push(entry);
                }
                Ok(RawPairs(pairs))
            }
        }

        deserializer.deserialize_map(PairVisitor)
    }
}
