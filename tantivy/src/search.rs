//! Searching splits: a tantivy query string, or OpenSearch query DSL (`dsl`),
//! over one or more splits, for hits, counts or aggregations. Several splits
//! merge natively: hits by score, counts summed, aggregations through
//! tantivy's intermediate results. Each split scores with its own term
//! statistics unless `global_stats` asks for the splits' combined ones.
//!
//! An `Exclude` set leaves out documents by a fast field's value, before
//! `top_k`, counts and aggregations: dead row ids, for per-snapshot liveness.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use roaring::{RoaringBitmap, RoaringTreemap};
use serde::Deserialize;
use serde_json::{Map, Value};
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::aggregation::{
    AggContextParams, AggregationLimitsGuard, DistributedAggregationCollector,
};
use tantivy::collector::{Collector, Count, FilterCollector, SegmentCollector, TopDocs};
use tantivy::columnar::{Cardinality, DynamicColumn};
use tantivy::query::{
    Bm25StatisticsProvider, ConstScorer, EnableScoring, Explanation, Query, QueryParser, Scorer,
    Weight,
};
use tantivy::schema::{Field, FieldType, Schema};
use tantivy::{
    DocAddress, DocId, DocSet, Document, Score, Searcher, SegmentOrdinal, SegmentReader,
    TantivyDocument, Term, TERMINATED,
};

use crate::split::{options, Split};

/// Search options, as JSON.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchOptions {
    /// The number of top hits, across all the splits (default: every hit).
    top_k: Option<usize>,
    /// The default fields for query-string terms that name none.
    fields: Option<Vec<String>>,
    #[serde(default)]
    conjunctive: bool,
    #[serde(default)]
    strict: bool,
    /// Build each hit's doc from these fast fields, not the stored fields.
    fast: Option<Vec<String>>,
    /// The fast field (i64 or u64) whose values `Exclude` holds.
    exclude_field: Option<String>,
    /// Score with the splits' combined term statistics.
    #[serde(default)]
    global_stats: bool,
}

/// Values of a fast field whose documents to leave out.
#[derive(Clone)]
pub struct Exclude(Arc<RoaringTreemap>);

impl Exclude {
    /// A serialized roaring bitmap, 32-bit or 64-bit (treemap), in the
    /// portable format.
    pub fn from_roaring(bytes: &[u8]) -> Result<Exclude> {
        let cookie = bytes
            .get(..4)
            .map(|b| u32::from_le_bytes(b.try_into().unwrap()));
        let bitmap = || -> Result<RoaringTreemap> {
            let mut remaining = bytes;
            let set = RoaringBitmap::deserialize_from(&mut remaining)?;
            anyhow::ensure!(remaining.is_empty(), "trailing bytes in exclude bitmap");
            Ok(RoaringTreemap::from_bitmaps([(0, set)]))
        };
        let treemap = || -> Result<RoaringTreemap> {
            let mut remaining = bytes;
            let set = RoaringTreemap::deserialize_from(&mut remaining)
                .context("exclude is not a serialized roaring bitmap")?;
            anyhow::ensure!(remaining.is_empty(), "trailing bytes in exclude bitmap");
            Ok(set)
        };
        let set = match cookie {
            // A treemap's container count can have a bitmap's cookie prefix.
            Some(c) if c == 12346 || c & 0xFFFF == 12347 => {
                bitmap().or_else(|e| treemap().map_err(|_| e))?
            }
            _ => treemap()?,
        };
        Ok(Exclude(Arc::new(set)))
    }

    pub fn from_ids(ids: impl IntoIterator<Item = i64>) -> Exclude {
        Exclude(Arc::new(ids.into_iter().map(|id| id as u64).collect()))
    }

    fn contains_i64(&self, v: i64) -> bool {
        self.0.contains(v as u64)
    }
}

/// A segment's open fast fields, by name.
type FastColumns = Vec<(String, Vec<DynamicColumn>)>;

/// The exclusion for one split: the field must be an i64 or u64 fast field.
pub(crate) enum Filter {
    None,
    I64(String, Exclude),
    U64(String, Exclude),
}

impl Filter {
    pub(crate) fn new(
        schema: &Schema,
        field: Option<&str>,
        exclude: Option<&Exclude>,
    ) -> Result<Filter> {
        let Some(exclude) = exclude else {
            return Ok(Filter::None);
        };
        let name = field.context("exclude needs options.exclude_field")?;
        let entry = schema.get_field_entry(schema.get_field(name)?);
        anyhow::ensure!(
            entry.is_fast(),
            "exclude_field {name:?} is not a fast field"
        );
        Ok(match entry.field_type() {
            FieldType::I64(_) => Filter::I64(name.to_owned(), exclude.clone()),
            FieldType::U64(_) => Filter::U64(name.to_owned(), exclude.clone()),
            _ => anyhow::bail!("exclude_field {name:?} must be an i64 or u64 field"),
        })
    }

    // FilterCollector tests one value at a time and rejects missing values.
    // An exclusion key must therefore have exactly one value per document.
    pub(crate) fn validate(&self, searcher: &Searcher) -> Result<()> {
        for reader in searcher.segment_readers() {
            let (name, cardinality) = match self {
                Filter::None => return Ok(()),
                Filter::I64(name, _) => (
                    name,
                    reader.fast_fields().i64(name)?.index.get_cardinality(),
                ),
                Filter::U64(name, _) => (
                    name,
                    reader.fast_fields().u64(name)?.index.get_cardinality(),
                ),
            };
            anyhow::ensure!(
                cardinality == Cardinality::Full,
                "exclude_field {name:?} must have exactly one value on every document"
            );
        }
        Ok(())
    }
}

fn collect<C: Collector>(
    searcher: &Searcher,
    query: &dyn Query,
    collector: C,
    filter: &Filter,
    stats: Option<&dyn Bm25StatisticsProvider>,
) -> Result<C::Fruit> {
    fn run<C: Collector>(
        searcher: &Searcher,
        query: &dyn Query,
        collector: &C,
        stats: Option<&dyn Bm25StatisticsProvider>,
    ) -> tantivy::Result<C::Fruit> {
        match stats {
            Some(stats) => searcher.search_with_statistics_provider(query, collector, stats),
            None => searcher.search(query, collector),
        }
    }
    Ok(match filter {
        Filter::None => run(searcher, query, &collector, stats)?,
        Filter::I64(field, ex) => {
            let ex = ex.clone();
            let keep = move |v: i64| !ex.contains_i64(v);
            run(
                searcher,
                query,
                &FilterCollector::new(field.clone(), keep, collector),
                stats,
            )?
        }
        Filter::U64(field, ex) => {
            let ex = ex.clone();
            let keep = move |v: u64| !ex.0.contains(v);
            run(
                searcher,
                query,
                &FilterCollector::new(field.clone(), keep, collector),
                stats,
            )?
        }
    })
}

/// A query string in tantivy's syntax, or (starting with `{`) OpenSearch
/// query DSL.
fn query(split: &Split, text: &str, options: &SearchOptions) -> Result<Box<dyn Query>> {
    if text.trim_start().starts_with('{') {
        let dsl: Value = serde_json::from_str(text).context("OpenSearch query DSL")?;
        return crate::dsl::compile(&split.index, &dsl);
    }
    let schema = split.index.schema();
    let fields = match &options.fields {
        Some(names) => names
            .iter()
            .map(|n| schema.get_field(n))
            .collect::<Result<_, _>>()?,
        None => text_fields(&schema),
    };
    let mut parser = QueryParser::for_index(&split.index, fields);
    if options.conjunctive {
        parser.set_conjunction_by_default();
    }
    Ok(if options.strict {
        parser.parse_query(text)?
    } else {
        parser.parse_query_lenient(text).0
    })
}

/// Every indexed text field: the default for terms that name no field.
pub(crate) fn text_fields(schema: &Schema) -> Vec<Field> {
    schema
        .fields()
        .filter(|(_, e)| {
            e.is_indexed() && matches!(e.field_type(), FieldType::Str(_) | FieldType::JsonObject(_))
        })
        .map(|(f, _)| f)
        .collect()
}

/// BM25 statistics summed over several splits (of one schema).
struct GlobalStats<'a>(&'a [Searcher]);

impl Bm25StatisticsProvider for GlobalStats<'_> {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        self.0.iter().map(|s| s.total_num_tokens(field)).sum()
    }
    fn total_num_docs(&self) -> tantivy::Result<u64> {
        self.0.iter().map(|s| s.total_num_docs()).sum()
    }
    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        self.0.iter().map(|s| s.doc_freq(term)).sum()
    }
}

/// A search request over some splits.
pub struct Request<'a> {
    splits: &'a [&'a Split],
    searchers: Vec<Searcher>,
    text: &'a str,
    options: SearchOptions,
    exclude: Option<&'a Exclude>,
}

impl<'a> Request<'a> {
    pub fn new(
        splits: &'a [&'a Split],
        text: &'a str,
        options_json: &str,
        exclude: Option<&'a Exclude>,
    ) -> Result<Request<'a>> {
        let options: SearchOptions = options(options_json, "search")?;
        if options.global_stats {
            if let Some(first) = splits.first() {
                anyhow::ensure!(
                    splits
                        .iter()
                        .all(|s| s.index.schema() == first.index.schema()),
                    "global_stats needs splits of the same schema"
                );
            }
        }
        if let Some(names) = &options.fast {
            for split in splits {
                let schema = split.index.schema();
                for name in names {
                    let (field, path) = schema
                        .find_field(name)
                        .with_context(|| format!("no fast field {name:?}"))?;
                    let entry = schema.get_field_entry(field);
                    anyhow::ensure!(
                        entry.is_fast()
                            && (path.is_empty()
                                || matches!(entry.field_type(), FieldType::JsonObject(_))),
                        "{name:?} is not a fast field"
                    );
                }
            }
        }
        Ok(Request {
            splits,
            searchers: splits.iter().map(|s| s.reader.searcher()).collect(),
            text,
            options,
            exclude,
        })
    }

    /// Run `per_split` on each split with its query and filter.
    fn each(
        &self,
        mut per_split: impl FnMut(
            usize,
            &Searcher,
            &dyn Query,
            &Filter,
            Option<&dyn Bm25StatisticsProvider>,
        ) -> Result<()>,
    ) -> Result<()> {
        let global = GlobalStats(&self.searchers);
        let stats: Option<&dyn Bm25StatisticsProvider> =
            (self.options.global_stats && self.splits.len() > 1).then_some(&global);
        for (i, (split, searcher)) in self.splits.iter().zip(&self.searchers).enumerate() {
            let q = query(split, self.text, &self.options)?;
            let schema = split.index.schema();
            let filter = Filter::new(&schema, self.options.exclude_field.as_deref(), self.exclude)?;
            filter.validate(searcher)?;
            per_split(i, searcher, &*q, &filter, stats)?;
        }
        Ok(())
    }

    /// Hits, best first: (split, score, doc as a JSON object).
    pub fn hits(&self) -> Result<Vec<(usize, Score, String)>> {
        let top_k = self.options.top_k;
        let mut found: Vec<(Score, usize, DocAddress)> = Vec::new();
        self.each(|i, searcher, q, filter, stats| {
            let hits = match top_k {
                Some(0) => Vec::new(),
                // TopDocs allocates for k: no more than the split holds.
                Some(k) => {
                    let k = k.min(searcher.num_docs() as usize).max(1);
                    collect(
                        searcher,
                        q,
                        TopDocs::with_limit(k).order_by_score(),
                        filter,
                        stats,
                    )?
                }
                None => collect(searcher, q, AllHits, filter, stats)?,
            };
            found.extend(hits.into_iter().map(|(score, addr)| (score, i, addr)));
            Ok(())
        })?;
        found.sort_by(|a, b| b.0.total_cmp(&a.0));
        if let Some(k) = top_k {
            found.truncate(k);
        }
        let mut columns: HashMap<(usize, SegmentOrdinal), FastColumns> = HashMap::new();
        found
            .into_iter()
            .map(|(score, i, addr)| {
                let searcher = &self.searchers[i];
                let doc = match &self.options.fast {
                    Some(names) => {
                        let cols = match columns.entry((i, addr.segment_ord)) {
                            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                            std::collections::hash_map::Entry::Vacant(e) => e.insert(fast_columns(
                                searcher.segment_reader(addr.segment_ord),
                                names,
                            )?),
                        };
                        fast_json(cols, addr.doc_id)?
                    }
                    None => stored_json(&searcher.doc(addr)?, &self.splits[i].index.schema())?,
                };
                Ok((i, score, doc))
            })
            .collect()
    }

    /// The number of matches.
    pub fn count(&self) -> Result<u64> {
        let mut count = 0;
        self.each(|_, searcher, q, filter, _| {
            count += collect(searcher, q, Count, filter, None)? as u64;
            Ok(())
        })?;
        Ok(count)
    }

    /// Tantivy aggregations (Elasticsearch's JSON) over the matches.
    pub fn aggregate(&self, aggs_json: &str) -> Result<String> {
        let aggs: Aggregations = serde_json::from_str(aggs_json).context("tantivy aggregations")?;
        let mut merged = IntermediateAggregationResults::default();
        let limits = AggregationLimitsGuard::default();
        self.each(|i, searcher, q, filter, _| {
            let context =
                AggContextParams::new(limits.clone(), self.splits[i].index.tokenizers().clone());
            let result = collect(
                searcher,
                q,
                DistributedAggregationCollector::from_aggs(aggs.clone(), context),
                filter,
                None,
            )?;
            merged.merge_fruits(result)?;
            Ok(())
        })?;
        Ok(serde_json::to_string(
            &merged.into_final_result(aggs, limits)?,
        )?)
    }
}

/// Stored fields as a JSON object: a field's value, or an array of several.
fn stored_json(doc: &TantivyDocument, schema: &Schema) -> Result<String> {
    let fields = doc
        .to_named_doc(schema)
        .0
        .into_iter()
        .map(|(name, mut values)| {
            let value = if values.len() == 1 {
                serde_json::to_value(values.pop())
            } else {
                serde_json::to_value(values)
            };
            value.map(|v| (name, v))
        })
        .collect::<Result<Map<_, _>, _>>()?;
    Ok(serde_json::to_string(&fields)?)
}

fn fast_columns(reader: &SegmentReader, names: &[String]) -> Result<FastColumns> {
    names
        .iter()
        .map(|name| {
            let columns = reader
                .fast_fields()
                .dynamic_column_handles(name)?
                .into_iter()
                .map(|h| h.open())
                .collect::<std::io::Result<Vec<_>>>()?;
            Ok((name.clone(), columns))
        })
        .collect()
}

/// Fast field values as a JSON object, like `stored_json`; fields without a
/// value are left out.
fn fast_json(columns: &FastColumns, doc: DocId) -> Result<String> {
    use tantivy::time::format_description::well_known::Rfc3339;
    let mut fields = Map::new();
    for (name, cols) in columns {
        let mut values = Vec::new();
        for col in cols {
            match col {
                DynamicColumn::Bool(c) => values.extend(c.values_for_doc(doc).map(Value::from)),
                DynamicColumn::I64(c) => values.extend(c.values_for_doc(doc).map(Value::from)),
                DynamicColumn::U64(c) => values.extend(c.values_for_doc(doc).map(Value::from)),
                DynamicColumn::F64(c) => values.extend(c.values_for_doc(doc).map(Value::from)),
                DynamicColumn::DateTime(c) => {
                    for d in c.values_for_doc(doc) {
                        values.push(Value::from(d.into_utc().format(&Rfc3339)?));
                    }
                }
                DynamicColumn::IpAddr(c) => {
                    values.extend(c.values_for_doc(doc).map(|ip| Value::from(ip.to_string())))
                }
                DynamicColumn::Str(c) => {
                    for ord in c.term_ords(doc) {
                        let mut s = String::new();
                        c.ord_to_str(ord, &mut s)?;
                        values.push(Value::from(s));
                    }
                }
                DynamicColumn::Bytes(c) => {
                    for ord in c.term_ords(doc) {
                        let mut b = Vec::new();
                        c.ord_to_bytes(ord, &mut b)?;
                        values.push(serde_json::to_value(tantivy::schema::OwnedValue::Bytes(b))?);
                    }
                }
            }
        }
        match values.len() {
            0 => {}
            1 => {
                fields.insert(name.clone(), values.pop().unwrap());
            }
            _ => {
                fields.insert(name.clone(), Value::Array(values));
            }
        }
    }
    Ok(serde_json::to_string(&fields)?)
}

/// Every match and its score, best first (`TopDocs` needs a limit).
struct AllHits;

struct SegmentHits(SegmentOrdinal, Vec<(Score, DocAddress)>);

impl Collector for AllHits {
    type Fruit = Vec<(Score, DocAddress)>;
    type Child = SegmentHits;

    fn for_segment(
        &self,
        segment: SegmentOrdinal,
        _: &SegmentReader,
    ) -> tantivy::Result<SegmentHits> {
        Ok(SegmentHits(segment, Vec::new()))
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        let mut hits: Vec<_> = fruits.into_iter().flatten().collect();
        hits.sort_by(|a, b| b.0.total_cmp(&a.0));
        Ok(hits)
    }
}

impl SegmentCollector for SegmentHits {
    type Fruit = Vec<(Score, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.1.push((score, DocAddress::new(self.0, doc)));
    }

    fn harvest(self) -> Self::Fruit {
        self.1
    }
}

/// Matches documents whose fast field value is in a set, by scanning the
/// field: for deleting excluded documents in a merge, which reads every
/// document anyway. (Searches filter matches instead.)
#[derive(Clone, Debug)]
pub(crate) struct InSetQuery {
    field: String,
    set: Arc<RoaringTreemap>,
}

impl InSetQuery {
    pub(crate) fn new(field: String, exclude: &Exclude) -> InSetQuery {
        InSetQuery {
            field,
            set: exclude.0.clone(),
        }
    }
}

impl Query for InSetQuery {
    fn weight(&self, _: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(self.clone()))
    }
}

impl Weight for InSetQuery {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let fast = reader.fast_fields();
        let mut docs = Vec::new();
        if let Some(col) = fast.column_opt::<i64>(&self.field)? {
            for doc in 0..reader.max_doc() {
                if col.values_for_doc(doc).any(|v| self.set.contains(v as u64)) {
                    docs.push(doc);
                }
            }
        } else if let Some(col) = fast.column_opt::<u64>(&self.field)? {
            for doc in 0..reader.max_doc() {
                if col.values_for_doc(doc).any(|v| self.set.contains(v)) {
                    docs.push(doc);
                }
            }
        }
        Ok(Box::new(ConstScorer::new(Docs { docs, at: 0 }, boost)))
    }

    fn explain(&self, _: &SegmentReader, _: DocId) -> tantivy::Result<Explanation> {
        Err(tantivy::TantivyError::InvalidArgument(
            "an exclusion is not explained".into(),
        ))
    }
}

/// A sorted list of documents.
struct Docs {
    docs: Vec<DocId>,
    at: usize,
}

impl DocSet for Docs {
    fn advance(&mut self) -> DocId {
        self.at += 1;
        self.doc()
    }

    fn doc(&self) -> DocId {
        self.docs.get(self.at).copied().unwrap_or(TERMINATED)
    }

    fn size_hint(&self) -> u32 {
        self.docs.len() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::split::testing::split;

    const DOCS: &[&str] = &[
        r#"{"id": 1, "body": "The quick brown fox jumps", "tag": "a"}"#,
        r#"{"id": 2, "body": "Small CATS and a café", "tag": ["b", "c"]}"#,
        r#"{"id": 3, "body": "cats chasing the fox", "tag": "a"}"#,
    ];

    fn hits(
        splits: &[&Split],
        query: &str,
        options: &str,
        exclude: Option<&Exclude>,
    ) -> Vec<(usize, f32, String)> {
        Request::new(splits, query, options, exclude)
            .unwrap()
            .hits()
            .unwrap()
    }

    fn docs(hits: Vec<(usize, f32, String)>) -> Vec<String> {
        hits.into_iter().map(|h| h.2).collect()
    }

    #[test]
    fn searches_with_custom_tokenizers_and_options() {
        let s = split(DOCS);
        let s = &[&s];
        // Stemmed, lowercased and folded; stored fields flatten single values.
        assert_eq!(
            docs(hits(s, "cat", "", None)),
            [r#"{"id":2,"tag":["b","c"]}"#, r#"{"id":3,"tag":"a"}"#]
        );
        assert_eq!(
            docs(hits(s, "cafe", "", None)),
            [r#"{"id":2,"tag":["b","c"]}"#]
        );
        assert!(hits(s, "the", "", None).is_empty());
        assert_eq!(hits(s, "fox cats", "", None).len(), 3);
        assert_eq!(
            docs(hits(s, "fox cats", r#"{"top_k": 1}"#, None)),
            [r#"{"id":3,"tag":"a"}"#]
        );
        assert_eq!(
            hits(s, "fox cats", r#"{"top_k": 1000000000}"#, None).len(),
            3
        );
        assert_eq!(
            hits(s, "fox cats", r#"{"conjunctive": true}"#, None).len(),
            1
        );
        assert!(hits(s, "fox", r#"{"top_k": 0}"#, None).is_empty());
        assert_eq!(hits(s, "tag:a AND id:[2 TO 3]", "", None).len(), 1);
        assert_eq!(hits(s, "\"brown fox\"", "", None).len(), 1);
        assert_eq!(hits(s, "a", r#"{"fields": ["tag"]}"#, None).len(), 2);
        assert_eq!(hits(s, "fox nosuchfield:x", "", None).len(), 2);
        assert!(
            Request::new(s, "nosuchfield:x", r#"{"strict": true}"#, None)
                .unwrap()
                .hits()
                .is_err()
        );
        assert!(Request::new(s, "x", r#"{"limit": 1}"#, None).is_err());
        let scores: Vec<f32> = hits(s, "fox cats", "", None)
            .into_iter()
            .map(|h| h.1)
            .collect();
        assert!(scores.windows(2).all(|w| w[0] >= w[1]), "{scores:?}");
    }

    #[test]
    fn projects_fast_fields() {
        let s = split(DOCS);
        // From fast fields: no stored-field read; multi-valued as arrays.
        assert_eq!(
            docs(hits(&[&s], "cats", r#"{"fast": ["id", "tag"]}"#, None)),
            [r#"{"id":2,"tag":["b","c"]}"#, r#"{"id":3,"tag":"a"}"#]
        );
        assert!(Request::new(&[&s], "fox", r#"{"fast": ["absent"]}"#, None).is_err());
        assert!(Request::new(&[&s], "fox", r#"{"fast": ["body"]}"#, None).is_err());
    }

    #[test]
    fn projects_fast_field_types_and_excludes_u64_ids() {
        use crate::split::testing::{build_with, open};
        use tantivy::schema::{JsonObjectOptions, FAST, INDEXED, STORED};
        let mut schema = Schema::builder();
        schema.add_u64_field("u", FAST | STORED | INDEXED);
        schema.add_f64_field("f", FAST | STORED);
        schema.add_bool_field("b", FAST | STORED);
        schema.add_date_field("d", FAST | STORED);
        schema.add_ip_addr_field("ip", FAST | STORED);
        schema.add_bytes_field("bytes", FAST | STORED);
        schema.add_json_field("meta", JsonObjectOptions::default().set_fast(None));
        let schema = serde_json::to_string(&schema.build()).unwrap();
        let s = open(build_with(
            &schema,
            "",
            &[r#"{
            "u":18446744073709551615,"f":1.5,"b":true,"d":"2026-01-02T03:04:05Z",
            "ip":"::1","bytes":"YWJj","meta":{"flag":true}
        }"#],
        ));
        let values = docs(hits(
            &[&s],
            "*",
            r#"{"fast":["u","f","b","d","ip","bytes","meta.flag"]}"#,
            None,
        ));
        let doc: Value = serde_json::from_str(&values[0]).unwrap();
        assert_eq!(doc["u"].as_u64(), Some(u64::MAX));
        assert_eq!(doc["f"], 1.5);
        assert_eq!(doc["b"], true);
        assert_eq!(doc["d"], "2026-01-02T03:04:05Z");
        assert_eq!(doc["ip"], "::1");
        assert_eq!(doc["bytes"], "YWJj");
        assert_eq!(doc["meta.flag"], true);
        let mut bytes = Vec::new();
        RoaringTreemap::from_iter([u64::MAX])
            .serialize_into(&mut bytes)
            .unwrap();
        let ex = Exclude::from_roaring(&bytes).unwrap();
        assert_eq!(
            Request::new(&[&s], "*", r#"{"exclude_field":"u"}"#, Some(&ex))
                .unwrap()
                .count()
                .unwrap(),
            0
        );
        assert_eq!(
            crate::split::merge(&[&s], r#"{"exclude_field":"u"}"#, Some(&ex), |_| Ok(())).unwrap(),
            0
        );
    }

    #[test]
    fn excludes_before_top_k_counts_and_aggregations() {
        let s = split(DOCS);
        let s = &[&s];
        // Excluded before top_k: the best remaining hit, not an empty page.
        let all = docs(hits(s, "fox cats", r#"{"fast": ["id"]}"#, None));
        assert_eq!(all[0], r#"{"id":3}"#);
        let opts = r#"{"exclude_field": "id", "top_k": 1, "fast": ["id"]}"#;
        assert_eq!(
            docs(hits(s, "fox cats", opts, Some(&Exclude::from_ids([3])))),
            [all[1].clone()]
        );
        let mut bitmap = Vec::new();
        RoaringBitmap::from_iter([1u32, 3])
            .serialize_into(&mut bitmap)
            .unwrap();
        let mut treemap = Vec::new();
        RoaringTreemap::from_iter([2u64])
            .serialize_into(&mut treemap)
            .unwrap();
        let count = |ex: &Exclude| {
            Request::new(s, "fox cats", r#"{"exclude_field": "id"}"#, Some(ex))
                .unwrap()
                .count()
                .unwrap()
        };
        assert_eq!(count(&Exclude::from_roaring(&bitmap).unwrap()), 1);
        assert_eq!(count(&Exclude::from_roaring(&treemap).unwrap()), 2);
        assert!(Exclude::from_roaring(b"nope").is_err());
        // The field must be a fast integer field.
        assert!(Request::new(
            s,
            "fox",
            r#"{"exclude_field": "body"}"#,
            Some(&Exclude::from_ids([1]))
        )
        .unwrap()
        .hits()
        .is_err());
        assert!(Request::new(s, "fox", "", Some(&Exclude::from_ids([1])))
            .unwrap()
            .hits()
            .is_err());
        let aggs = Request::new(
            s,
            "*",
            r#"{"exclude_field": "id"}"#,
            Some(&Exclude::from_ids([1])),
        )
        .unwrap()
        .aggregate(
            r#"{"tags": {"terms": {"field": "tag"}}, "n": {"cardinality": {"field": "id"}}}"#,
        )
        .unwrap();
        let aggs: Value = serde_json::from_str(&aggs).unwrap();
        let buckets: Vec<(String, u64)> = aggs["tags"]["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| {
                (
                    b["key"].as_str().unwrap().to_owned(),
                    b["doc_count"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(buckets, [("a".into(), 1), ("b".into(), 1), ("c".into(), 1)]);
        assert_eq!(aggs["n"]["value"].as_f64().unwrap().round(), 2.0);
    }

    #[test]
    fn validates_exclusion_keys_and_bitmap_bytes() {
        for docs in [
            vec![r#"{"id":1,"body":"fox"}"#, r#"{"body":"fox"}"#],
            vec![r#"{"id":[1,2],"body":"fox"}"#],
        ] {
            let s = split(&docs);
            let ex = Exclude::from_ids([1]);
            assert!(
                Request::new(&[&s], "fox", r#"{"exclude_field":"id"}"#, Some(&ex))
                    .unwrap()
                    .count()
                    .is_err()
            );
            assert!(
                crate::split::merge(&[&s], r#"{"exclude_field":"id"}"#, Some(&ex), |_| Ok(()))
                    .is_err()
            );
        }
        let s = split(&[
            r#"{"id":-1,"body":"fox"}"#,
            r#"{"id":1099511627776,"body":"fox"}"#,
        ]);
        let ex = Exclude::from_ids([-1]);
        assert_eq!(
            Request::new(&[&s], "fox", r#"{"exclude_field":"id"}"#, Some(&ex))
                .unwrap()
                .count()
                .unwrap(),
            1
        );
        let mut bytes = Vec::new();
        RoaringTreemap::from_iter([1u64 << 40])
            .serialize_into(&mut bytes)
            .unwrap();
        assert_eq!(
            Request::new(
                &[&s],
                "fox",
                r#"{"exclude_field":"id"}"#,
                Some(&Exclude::from_roaring(&bytes).unwrap())
            )
            .unwrap()
            .count()
            .unwrap(),
            1
        );
        bytes.push(0);
        assert!(Exclude::from_roaring(&bytes).is_err());
        for bytes in [vec![0; 8], vec![58, 48, 0, 0, 0, 0, 0, 0]] {
            assert_eq!(Exclude::from_roaring(&bytes).unwrap().0.len(), 0);
        }
        let set: RoaringTreemap = (0..12346u64).map(|i| i << 32).collect();
        let mut bytes = Vec::new();
        set.serialize_into(&mut bytes).unwrap();
        assert_eq!(*Exclude::from_roaring(&bytes).unwrap().0, set);
    }

    #[test]
    fn merges_hits_counts_and_aggregations_over_splits() {
        let a = split(&DOCS[..2]);
        let b = split(&DOCS[2..]);
        let splits = &[&a, &b];
        let found = hits(splits, "fox cats", r#"{"fast": ["id"]}"#, None);
        assert_eq!(found.len(), 3);
        assert!(found.windows(2).all(|w| w[0].1 >= w[1].1));
        assert_eq!(found.iter().filter(|h| h.0 == 1).count(), 1);
        assert_eq!(hits(splits, "fox cats", r#"{"top_k": 2}"#, None).len(), 2);
        assert_eq!(
            Request::new(splits, "fox OR cats", "", None)
                .unwrap()
                .count()
                .unwrap(),
            3
        );
        let aggs = Request::new(splits, "*", "", None)
            .unwrap()
            .aggregate(r#"{"tags": {"terms": {"field": "tag"}}}"#)
            .unwrap();
        assert!(aggs.contains(r#""key":"a","doc_count":2"#), "{aggs}");
        // Global statistics: a term's score no longer depends on which split holds it.
        let local = hits(&[&a, &b], "fox", r#"{"fast": ["id"]}"#, None);
        let global = hits(
            &[&a, &b],
            "fox",
            r#"{"fast": ["id"], "global_stats": true}"#,
            None,
        );
        let whole = split(DOCS);
        let reference = hits(&[&whole], "fox", r#"{"fast": ["id"]}"#, None);
        let score =
            |h: &[(usize, f32, String)], id: &str| h.iter().find(|x| x.2.contains(id)).unwrap().1;
        assert!((score(&global, "\"id\":1") - score(&reference, "\"id\":1")).abs() < 1e-4);
        assert!((score(&local, "\"id\":1") - score(&reference, "\"id\":1")).abs() > 1e-4);
        assert!(Request::new(&[], "fox", "", None)
            .unwrap()
            .hits()
            .unwrap()
            .is_empty());
        assert_eq!(
            Request::new(&[], "fox", "", None).unwrap().count().unwrap(),
            0
        );
    }
}
