//! Searching splits: a tantivy query string, or OpenSearch query DSL (`dsl`),
//! over one or more splits, for hits, counts or aggregations. Splits are
//! searched in parallel and merged natively: hits by score, counts summed,
//! aggregations through tantivy's intermediate results. Each split scores with
//! its own term statistics unless `global_stats` asks for the splits' combined
//! ones.
//!
//! An `Exclude` set leaves out documents by a fast field's value: dead row
//! ids, for per-snapshot liveness. It becomes a per-segment alive bitset, as
//! tantivy's own deletes are, built once per split and set and kept, so
//! scoring skips dead documents and `top_k`, counts and aggregations never
//! see them.

use std::cmp::Ordering as Cmp;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{BuildHasher, Hasher, RandomState};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{anyhow, bail, ensure, Context as _, Result};
use roaring::{RoaringBitmap, RoaringTreemap};
use serde::Deserialize;
use serde_json::{Map, Value};
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::aggregation::{
    AggContextParams, AggregationLimitsGuard, DistributedAggregationCollector,
};
use tantivy::collector::{Collector, Count, SegmentCollector, TopDocs};
use tantivy::columnar::{Cardinality, Column, DynamicColumn, StrColumn};
use tantivy::directory::OwnedBytes;
use tantivy::fastfield::{write_alive_bitset, AliveBitSet};
use tantivy::query::{
    Bm25StatisticsProvider, ConstScorer, EnableScoring, Explanation, Query, QueryParser, Scorer,
    Weight,
};
use tantivy::schema::{Field, FieldType, Schema};
use tantivy::snippet::SnippetGenerator;
use tantivy::{
    f64_to_u64, i64_to_u64, u64_to_i64, DateTime, DocAddress, DocId, DocSet, Document, Order,
    Score, Searcher, SegmentOrdinal, SegmentReader, TantivyDocument, Term, TERMINATED,
};
use tantivy_common::BitSet;

use crate::split::{options, Split};

/// Search options, as JSON.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchOptions {
    /// The number of top hits, across all the splits (default: every hit).
    top_k: Option<usize>,
    /// Hits to skip before `top_k`, across all the splits.
    #[serde(default)]
    offset: usize,
    /// Counts only: stop after this many matches; the result is then
    /// `limit + 1` if there are more.
    limit: Option<u64>,
    /// Counts only: the exact number of distinct values of this fast field.
    distinct: Option<String>,
    /// Hits only: the best hit of each distinct value of this fast field.
    collapse: Option<String>,
    /// Hits only: order by a fast field (numbers, dates, text), not by score.
    sort: Option<SortSpec>,
    /// Hits only: HTML snippets of stored text fields, as the query matches them.
    highlight: Option<HighlightSpec>,
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
    /// In OpenSearch query DSL, a field a split lacks matches nothing.
    #[serde(default)]
    ignore_unmapped: bool,
}

/// `"price"` (ascending), or `{"field": "price", "order": "desc"}`.
#[derive(Deserialize)]
#[serde(untagged)]
enum SortSpec {
    Field(String),
    By(SortBy),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SortBy {
    field: String,
    #[serde(default)]
    order: SortOrder,
}

#[derive(Deserialize, Default, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
enum SortOrder {
    #[default]
    Asc,
    Desc,
}

impl SortSpec {
    fn field(&self) -> &str {
        match self {
            SortSpec::Field(field) => field,
            SortSpec::By(by) => &by.field,
        }
    }

    fn order(&self) -> SortOrder {
        match self {
            SortSpec::Field(_) => SortOrder::Asc,
            SortSpec::By(by) => by.order,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HighlightSpec {
    fields: Vec<String>,
    /// The longest snippet (default: tantivy's 150).
    max_chars: Option<usize>,
}

/// Values of a fast field whose documents to leave out.
#[derive(Clone)]
pub struct Exclude {
    set: Arc<RoaringTreemap>,
    /// Identifies the input, for the alive bitsets built from it.
    digest: u128,
}

fn digest(kind: u8, bytes: &[u8]) -> u128 {
    static KEYS: OnceLock<(RandomState, RandomState)> = OnceLock::new();
    let (a, b) = KEYS.get_or_init(|| (RandomState::new(), RandomState::new()));
    let hash = |state: &RandomState| {
        let mut h = state.build_hasher();
        h.write_u8(kind);
        h.write_usize(bytes.len());
        h.write(bytes);
        h.finish()
    };
    (u128::from(hash(a)) << 64) | u128::from(hash(b))
}

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
            ensure!(remaining.is_empty(), "trailing bytes in exclude bitmap");
            Ok(RoaringTreemap::from_bitmaps([(0, set)]))
        };
        let treemap = || -> Result<RoaringTreemap> {
            let mut remaining = bytes;
            let set = RoaringTreemap::deserialize_from(&mut remaining)
                .context("exclude is not a serialized roaring bitmap")?;
            ensure!(remaining.is_empty(), "trailing bytes in exclude bitmap");
            Ok(set)
        };
        let set = match cookie {
            // A treemap's container count can have a bitmap's cookie prefix.
            Some(c) if c == 12346 || c & 0xFFFF == 12347 => {
                bitmap().or_else(|e| treemap().map_err(|_| e))?
            }
            _ => treemap()?,
        };
        Ok(Exclude {
            set: Arc::new(set),
            digest: digest(1, bytes),
        })
    }

    pub fn from_id_slice(ids: &[i64]) -> Exclude {
        // SAFETY: any initialized i64s are valid bytes; only hashed.
        let bytes =
            unsafe { std::slice::from_raw_parts(ids.as_ptr().cast::<u8>(), size_of_val(ids)) };
        Exclude {
            set: Arc::new(ids.iter().map(|&id| id as u64).collect()),
            digest: digest(2, bytes),
        }
    }

    pub fn from_ids(ids: impl IntoIterator<Item = i64>) -> Exclude {
        Exclude::from_id_slice(&ids.into_iter().collect::<Vec<_>>())
    }
}

/// The key `exclude_field` names: `true` for an i64 field, `false` for u64.
fn key_is_signed(schema: &Schema, name: &str) -> Result<bool> {
    let field = schema
        .get_field(name)
        .map_err(|_| anyhow!("no field {name:?}"))?;
    let entry = schema.get_field_entry(field);
    ensure!(
        entry.is_fast(),
        "exclude_field {name:?} is not a fast field"
    );
    match entry.field_type() {
        FieldType::I64(_) => Ok(true),
        FieldType::U64(_) => Ok(false),
        _ => bail!("exclude_field {name:?} must be an i64 or u64 field"),
    }
}

/// An exclusion key needs exactly one value on every document: a missing one
/// would be neither kept nor dropped by anything principled.
pub(crate) fn check_exclusion_key(split: &Split, name: &str) -> Result<()> {
    let signed = key_is_signed(&split.index.schema(), name)?;
    for reader in split.reader.searcher().segment_readers() {
        let cardinality = if signed {
            reader.fast_fields().i64(name)?.get_cardinality()
        } else {
            reader.fast_fields().u64(name)?.get_cardinality()
        };
        ensure!(
            cardinality == Cardinality::Full,
            "exclude_field {name:?} must have exactly one value on every document"
        );
    }
    Ok(())
}

/// A split's segments with an exclusion's documents deleted.
pub(crate) struct Alive {
    readers: Vec<SegmentReader>,
}

const BLOCK: usize = 4096;

fn mark_dead<T>(
    column: &Column<T>,
    max_doc: u32,
    is_dead: impl Fn(T) -> bool,
    alive: &mut BitSet,
) -> Result<()>
where
    T: PartialOrd + Copy + Default + std::fmt::Debug + Send + Sync + 'static,
{
    let mut values = [T::default(); BLOCK];
    for start in (0..max_doc).step_by(BLOCK) {
        let n = ((max_doc - start) as usize).min(BLOCK);
        column.values.get_range(u64::from(start), &mut values[..n]);
        for (offset, &value) in values[..n].iter().enumerate() {
            if is_dead(value) {
                alive.remove(start + offset as u32);
            }
        }
    }
    Ok(())
}

fn build_alive(split: &Split, name: &str, exclude: &Exclude) -> Result<Alive> {
    check_exclusion_key(split, name)?;
    let signed = key_is_signed(&split.index.schema(), name)?;
    let searcher = split.reader.searcher();
    let segments = split.index.searchable_segments()?;
    ensure!(
        segments.len() == searcher.segment_readers().len(),
        "the split changed while it was open"
    );
    let mut readers = Vec::with_capacity(segments.len());
    for (segment, reader) in segments.iter().zip(searcher.segment_readers()) {
        let max_doc = reader.max_doc();
        let mut alive = BitSet::with_max_value_and_full(max_doc);
        if signed {
            let column = reader.fast_fields().i64(name)?;
            mark_dead(
                &column,
                max_doc,
                |v| exclude.set.contains(v as u64),
                &mut alive,
            )?;
        } else {
            let column = reader.fast_fields().u64(name)?;
            mark_dead(&column, max_doc, |v| exclude.set.contains(v), &mut alive)?;
        }
        let mut bytes = Vec::new();
        write_alive_bitset(&alive, &mut bytes)?;
        readers.push(SegmentReader::open_with_custom_alive_set(
            segment,
            Some(AliveBitSet::open(OwnedBytes::new(bytes))),
        )?);
    }
    Ok(Alive { readers })
}

type AliveSlot = Arc<OnceLock<std::result::Result<Arc<Alive>, String>>>;

/// The alive bitsets of one split, for the last few exclusions searched.
#[derive(Default)]
pub(crate) struct AliveCache(Mutex<VecDeque<((String, u128), AliveSlot)>>);

const ALIVE_SETS_PER_SPLIT: usize = 4;

impl AliveCache {
    fn get(&self, split: &Split, name: &str, exclude: &Exclude) -> Result<Arc<Alive>> {
        let key = (name.to_owned(), exclude.digest);
        let slot = {
            let mut entries = self.0.lock().unwrap();
            match entries.iter().position(|(k, _)| *k == key) {
                Some(at) => {
                    let entry = entries.remove(at).unwrap();
                    let slot = entry.1.clone();
                    entries.push_back(entry);
                    slot
                }
                None => {
                    let slot = AliveSlot::default();
                    entries.push_back((key.clone(), slot.clone()));
                    if entries.len() > ALIVE_SETS_PER_SPLIT {
                        entries.pop_front();
                    }
                    slot
                }
            }
        };
        let built = slot.get_or_init(|| {
            build_alive(split, name, exclude)
                .map(Arc::new)
                .map_err(|e| format!("{e:#}"))
        });
        match built {
            Ok(alive) => Ok(alive.clone()),
            Err(message) => {
                // A failure (an unreadable file, say) may not repeat.
                let mut entries = self.0.lock().unwrap();
                entries.retain(|(k, s)| !(*k == key && Arc::ptr_eq(s, &slot)));
                Err(anyhow!("{message}"))
            }
        }
    }
}

/// One split being searched: its searcher, and its segments without the
/// documents an exclusion deletes.
struct Run<'a> {
    searcher: &'a Searcher,
    alive: Option<Arc<Alive>>,
}

impl Run<'_> {
    fn readers(&self) -> &[SegmentReader] {
        match &self.alive {
            Some(alive) => &alive.readers,
            None => self.searcher.segment_readers(),
        }
    }

    fn num_docs(&self) -> u64 {
        self.readers().iter().map(|r| u64::from(r.num_docs())).sum()
    }
}

type Stats<'a> = Option<&'a (dyn Bm25StatisticsProvider + Sync)>;

/// What `Searcher::search` does, over a run's segments.
fn collect<C: Collector>(
    run: &Run,
    query: &dyn Query,
    collector: &C,
    stats: Stats,
) -> Result<C::Fruit> {
    let scoring = match (collector.requires_scoring(), stats) {
        (false, _) => EnableScoring::disabled_from_searcher(run.searcher),
        (true, None) => EnableScoring::enabled_from_searcher(run.searcher),
        (true, Some(stats)) => EnableScoring::enabled_from_statistics_provider(stats, run.searcher),
    };
    let weight = query.weight(scoring)?;
    let fruits = run
        .readers()
        .iter()
        .enumerate()
        .map(|(ord, reader)| collector.collect_segment(&*weight, ord as u32, reader))
        .collect::<tantivy::Result<Vec<_>>>()?;
    Ok(collector.merge_fruits(fruits)?)
}

/// The matches of a run, counted up to `cap`.
fn count_up_to(run: &Run, query: &dyn Query, cap: u64) -> Result<u64> {
    let weight = query.weight(EnableScoring::disabled_from_searcher(run.searcher))?;
    let mut n = 0;
    for reader in run.readers() {
        let alive = reader.alive_bitset();
        let mut scorer = weight.scorer(reader, 1.0)?;
        let mut doc = scorer.doc();
        while doc != TERMINATED {
            if alive.is_none_or(|a| a.is_alive(doc)) {
                n += 1;
                if n >= cap {
                    return Ok(n);
                }
            }
            doc = scorer.advance();
        }
    }
    Ok(n)
}

/// A query string in tantivy's syntax, or (starting with `{`) OpenSearch
/// query DSL.
fn query(split: &Split, text: &str, options: &SearchOptions) -> Result<Box<dyn Query>> {
    if text.trim_start().starts_with('{') {
        let dsl: Value = serde_json::from_str(text).context("OpenSearch query DSL")?;
        return crate::dsl::compile(&split.index, &dsl, options.ignore_unmapped);
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

/// BM25 statistics summed over several splits (of one schema). Like
/// tantivy's own, they count a split's deleted (here: excluded) documents
/// until it is merged.
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

/// `f(0..n)` on up to `threads` threads; the results in order, or the first
/// error in order.
fn par_map<T: Send>(
    n: usize,
    threads: usize,
    f: impl Fn(usize) -> Result<T> + Sync,
) -> Result<Vec<T>> {
    let workers = threads.min(n);
    if workers <= 1 {
        return (0..n).map(f).collect();
    }
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let slots: Vec<Mutex<Option<Result<T>>>> = (0..n).map(|_| Mutex::new(None)).collect();
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= n || failed.load(Ordering::Relaxed) {
                    break;
                }
                let result = f(i);
                if result.is_err() {
                    failed.store(true, Ordering::Relaxed);
                }
                *slots[i].lock().unwrap() = Some(result);
            });
        }
    });
    slots
        .into_iter()
        .filter_map(|slot| slot.into_inner().unwrap())
        .collect()
}

/// What a sort field holds: the types tantivy sorts by.
#[derive(Clone, Copy, PartialEq, Debug)]
enum SortKind {
    I64,
    U64,
    F64,
    Date,
    Str,
}

fn sort_kind(schema: &Schema, name: &str) -> Result<SortKind> {
    let field = schema
        .get_field(name)
        .map_err(|_| anyhow!("sort: no field {name:?}"))?;
    let entry = schema.get_field_entry(field);
    ensure!(entry.is_fast(), "sort: {name:?} is not a fast field");
    Ok(match entry.field_type() {
        FieldType::I64(_) => SortKind::I64,
        FieldType::U64(_) => SortKind::U64,
        FieldType::F64(_) => SortKind::F64,
        FieldType::Date(_) => SortKind::Date,
        FieldType::Str(_) => SortKind::Str,
        _ => bail!("sort: {name:?} is not a number, a date or text"),
    })
}

/// A hit's value of the sort field; a document without one sorts last.
#[derive(Clone, Debug)]
enum SortKey {
    I64(Option<i64>),
    U64(Option<u64>),
    F64(Option<f64>),
    Date(Option<DateTime>),
    Str(Option<String>),
}

fn compare<T: PartialOrd>(a: &Option<T>, b: &Option<T>, order: SortOrder) -> Cmp {
    match (a, b) {
        (None, None) => Cmp::Equal,
        (None, Some(_)) => Cmp::Greater,
        (Some(_), None) => Cmp::Less,
        (Some(a), Some(b)) => {
            let by = a.partial_cmp(b).unwrap_or(Cmp::Equal);
            if order == SortOrder::Desc {
                by.reverse()
            } else {
                by
            }
        }
    }
}

impl SortKey {
    fn compare(&self, other: &SortKey, order: SortOrder) -> Cmp {
        match (self, other) {
            (SortKey::I64(a), SortKey::I64(b)) => compare(a, b, order),
            (SortKey::U64(a), SortKey::U64(b)) => compare(a, b, order),
            (SortKey::F64(a), SortKey::F64(b)) => compare(a, b, order),
            (SortKey::Date(a), SortKey::Date(b)) => compare(a, b, order),
            (SortKey::Str(a), SortKey::Str(b)) => compare(a, b, order),
            _ => Cmp::Equal, // one type across splits is checked up front
        }
    }
}

/// The `k` best documents of a run by a fast field.
fn sorted(
    run: &Run,
    query: &dyn Query,
    stats: Stats,
    spec: &SortSpec,
    k: usize,
) -> Result<Vec<(SortKey, DocAddress)>> {
    let order = match spec.order() {
        SortOrder::Asc => Order::Asc,
        SortOrder::Desc => Order::Desc,
    };
    let field = spec.field();
    let top = TopDocs::with_limit(k);
    Ok(match sort_kind(run.searcher.schema(), field)? {
        SortKind::I64 => collect(
            run,
            query,
            &top.order_by_fast_field::<i64>(field, order),
            stats,
        )?
        .into_iter()
        .map(|(v, a)| (SortKey::I64(v), a))
        .collect(),
        SortKind::U64 => collect(
            run,
            query,
            &top.order_by_fast_field::<u64>(field, order),
            stats,
        )?
        .into_iter()
        .map(|(v, a)| (SortKey::U64(v), a))
        .collect(),
        SortKind::F64 => collect(
            run,
            query,
            &top.order_by_fast_field::<f64>(field, order),
            stats,
        )?
        .into_iter()
        .map(|(v, a)| (SortKey::F64(v), a))
        .collect(),
        SortKind::Date => collect(
            run,
            query,
            &top.order_by_fast_field::<DateTime>(field, order),
            stats,
        )?
        .into_iter()
        .map(|(v, a)| (SortKey::Date(v), a))
        .collect(),
        SortKind::Str => collect(
            run,
            query,
            &top.order_by_string_fast_field(field, order),
            stats,
        )?
        .into_iter()
        .map(|(v, a)| (SortKey::Str(v), a))
        .collect(),
    })
}

/// A hit: the position of its split in the list, its score (NaN when hits are
/// ordered by a field), its document as a JSON object, and its snippets.
#[derive(Clone, Debug)]
pub struct Hit {
    pub split: usize,
    pub score: Score,
    pub doc: String,
    pub highlight: Option<String>,
}

impl PartialEq for Hit {
    fn eq(&self, other: &Hit) -> bool {
        // Bit for bit, so that NaN scores of two equal searches are equal.
        self.split == other.split
            && self.score.to_bits() == other.score.to_bits()
            && self.doc == other.doc
            && self.highlight == other.highlight
    }
}

/// A search request over some splits.
pub struct Request<'a> {
    splits: &'a [&'a Split],
    searchers: Vec<Searcher>,
    text: &'a str,
    options: SearchOptions,
    exclude: Option<&'a Exclude>,
    threads: usize,
}

/// A hit before its document is read.
struct Found {
    score: Score,
    split: usize,
    addr: DocAddress,
    key: Option<Key>,
    sort: Option<SortKey>,
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
                ensure!(
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
                    ensure!(
                        entry.is_fast()
                            && (path.is_empty()
                                || matches!(entry.field_type(), FieldType::JsonObject(_))),
                        "{name:?} is not a fast field"
                    );
                }
            }
        }
        if let Some(sort) = &options.sort {
            ensure!(
                options.collapse.is_none(),
                "sort and collapse cannot be combined"
            );
            let mut kind = None;
            for split in splits {
                let this = sort_kind(&split.index.schema(), sort.field())?;
                ensure!(
                    kind.is_none_or(|k| k == this),
                    "sort: {:?} has different types in the splits",
                    sort.field()
                );
                kind = Some(this);
            }
        }
        if let Some(highlight) = &options.highlight {
            ensure!(!highlight.fields.is_empty(), "highlight needs fields");
            for split in splits {
                let schema = split.index.schema();
                for name in &highlight.fields {
                    let field = schema
                        .get_field(name)
                        .map_err(|_| anyhow!("highlight: no field {name:?}"))?;
                    let entry = schema.get_field_entry(field);
                    ensure!(
                        matches!(entry.field_type(), FieldType::Str(_)) && entry.is_stored(),
                        "highlight: {name:?} is not a stored text field"
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
            threads: 1,
        })
    }

    /// Search up to this many splits at a time (default 1).
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads.max(1);
        self
    }

    fn run(&self, i: usize) -> Result<Run<'_>> {
        let alive = match (self.exclude, self.options.exclude_field.as_deref()) {
            (Some(exclude), Some(name)) => {
                Some(self.splits[i].alive.get(self.splits[i], name, exclude)?)
            }
            (Some(_), None) => bail!("exclude needs options.exclude_field"),
            (None, _) => None,
        };
        Ok(Run {
            searcher: &self.searchers[i],
            alive,
        })
    }

    /// Run `f` on each split with its run, query and statistics.
    fn each<T: Send>(
        &self,
        f: impl Fn(usize, &Run, &dyn Query, Stats) -> Result<T> + Sync,
    ) -> Result<Vec<T>> {
        let global = GlobalStats(&self.searchers);
        let stats: Stats = (self.options.global_stats && self.splits.len() > 1).then_some(&global);
        par_map(self.splits.len(), self.threads, |i| {
            let run = self.run(i)?;
            let q = query(self.splits[i], self.text, &self.options)?;
            f(i, &run, &*q, stats)
        })
    }

    /// Hits, best first: (split, score, doc as a JSON object).
    pub fn hits(&self) -> Result<Vec<Hit>> {
        let o = &self.options;
        ensure!(
            o.limit.is_none() && o.distinct.is_none(),
            "limit and distinct are options of tantivy_count"
        );
        let want = o.top_k.map(|k| o.offset.saturating_add(k));
        let per_split = self.each(|i, run, q, stats| {
            let found = |score, addr, key, sort| Found {
                score,
                split: i,
                addr,
                key,
                sort,
            };
            Ok(match (&o.sort, &o.collapse, want) {
                (_, _, Some(0)) => Vec::new(),
                (Some(sort), _, _) => {
                    let k = want.unwrap_or(usize::MAX).min(run.num_docs() as usize);
                    sorted(run, q, stats, sort, k.max(1))?
                        .into_iter()
                        .map(|(key, addr)| found(Score::NAN, addr, None, Some(key)))
                        .collect()
                }
                (None, Some(field), _) => {
                    let collapse = Collapse {
                        field: field.clone(),
                        groups: want.unwrap_or(usize::MAX),
                    };
                    collect(run, q, &collapse, stats)?
                        .into_iter()
                        .map(|(score, key, addr)| found(score, addr, Some(key), None))
                        .collect()
                }
                // TopDocs allocates for k: no more than the split holds.
                (None, None, Some(k)) => {
                    let k = k.min(run.num_docs() as usize).max(1);
                    let top = TopDocs::with_limit(k).order_by_score();
                    collect(run, q, &top, stats)?
                        .into_iter()
                        .map(|(score, addr)| found(score, addr, None, None))
                        .collect()
                }
                (None, None, None) => collect(run, q, &AllHits, stats)?
                    .into_iter()
                    .map(|(score, addr)| found(score, addr, None, None))
                    .collect(),
            })
        })?;
        let mut found: Vec<Found> = per_split.into_iter().flatten().collect();
        if o.collapse.is_some() {
            // A value's best hit over all the splits, in the order the splits
            // gave them, so equal scores keep it whatever the page.
            let mut best: HashMap<Key, usize> = HashMap::new();
            let mut groups: Vec<(usize, Found)> = Vec::new();
            for (at, hit) in found.into_iter().enumerate() {
                let key = hit.key.clone().unwrap_or(Key::Missing);
                match best.get(&key) {
                    Some(&g) if groups[g].1.score >= hit.score => {}
                    Some(&g) => groups[g] = (at, hit),
                    None => {
                        best.insert(key, groups.len());
                        groups.push((at, hit));
                    }
                }
            }
            groups.sort_by_key(|(at, _)| *at);
            found = groups.into_iter().map(|(_, hit)| hit).collect();
        }
        // Equal hits stay in split order, then the order a split gave them.
        match &o.sort {
            Some(sort) => found.sort_by(|a, b| match (&a.sort, &b.sort) {
                (Some(a), Some(b)) => a.compare(b, sort.order()),
                _ => Cmp::Equal,
            }),
            None => found.sort_by(|a, b| b.score.total_cmp(&a.score)),
        }
        let found: Vec<Found> = found
            .into_iter()
            .skip(o.offset)
            .take(o.top_k.unwrap_or(usize::MAX))
            .collect();
        // Documents, only for the hits kept, a split at a time.
        let mut positions: Vec<Vec<usize>> = vec![Vec::new(); self.splits.len()];
        for (at, hit) in found.iter().enumerate() {
            positions[hit.split].push(at);
        }
        let docs = par_map(self.splits.len(), self.threads, |i| {
            let searcher = &self.searchers[i];
            let schema = self.splits[i].index.schema();
            let snippets = match &o.highlight {
                Some(spec) => Some(self.snippet_generators(i, spec)?),
                None => None,
            };
            let mut columns: HashMap<SegmentOrdinal, FastColumns> = HashMap::new();
            positions[i]
                .iter()
                .map(|&at| {
                    let addr = found[at].addr;
                    // Stored fields are read for a document that needs them.
                    let stored = if o.fast.is_none() || snippets.is_some() {
                        Some(searcher.doc::<TantivyDocument>(addr)?)
                    } else {
                        None
                    };
                    let doc = match (&o.fast, &stored) {
                        (Some(names), _) => {
                            let cols = match columns.entry(addr.segment_ord) {
                                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                                std::collections::hash_map::Entry::Vacant(e) => e.insert(
                                    fast_columns(searcher.segment_reader(addr.segment_ord), names)?,
                                ),
                            };
                            fast_json(cols, addr.doc_id)?
                        }
                        (None, Some(stored)) => stored_json(stored, &schema)?,
                        (None, None) => unreachable!("stored fields are read without `fast`"),
                    };
                    let highlight = match (&snippets, &stored) {
                        (Some(generators), Some(stored)) => {
                            Some(highlight_json(generators, stored)?)
                        }
                        _ => None,
                    };
                    Ok((doc, highlight))
                })
                .collect::<Result<Vec<(String, Option<String>)>>>()
        })?;
        let mut out: Vec<Hit> = found
            .iter()
            .map(|hit| Hit {
                split: hit.split,
                score: hit.score,
                doc: String::new(),
                highlight: None,
            })
            .collect();
        for (split_positions, split_docs) in positions.iter().zip(docs) {
            for (&at, (doc, highlight)) in split_positions.iter().zip(split_docs) {
                out[at].doc = doc;
                out[at].highlight = highlight;
            }
        }
        Ok(out)
    }

    /// What marks the query's terms in a stored text field, for a split.
    fn snippet_generators(
        &self,
        i: usize,
        spec: &HighlightSpec,
    ) -> Result<Vec<(String, SnippetGenerator)>> {
        let split = self.splits[i];
        let q = query(split, self.text, &self.options)?;
        let schema = split.index.schema();
        spec.fields
            .iter()
            .map(|name| {
                let mut generator =
                    SnippetGenerator::create(&self.searchers[i], &*q, schema.get_field(name)?)?;
                if let Some(chars) = spec.max_chars {
                    generator.set_max_num_chars(chars);
                }
                Ok((name.clone(), generator))
            })
            .collect()
    }

    /// The number of matches: exact, or capped by `limit`, or the distinct
    /// values of a field.
    pub fn count(&self) -> Result<u64> {
        let o = &self.options;
        ensure!(
            o.offset == 0 && o.collapse.is_none() && o.sort.is_none() && o.highlight.is_none(),
            "offset, collapse, sort and highlight are options of tantivy_search"
        );
        ensure!(
            o.limit.is_none() || o.distinct.is_none(),
            "limit and distinct cannot be combined"
        );
        if let Some(field) = &o.distinct {
            let distinct = Distinct {
                field: field.clone(),
            };
            let mut all: HashSet<Key> = HashSet::new();
            for values in self.each(|_, run, q, _| collect(run, q, &distinct, None))? {
                all.extend(values);
            }
            return Ok(all.len() as u64);
        }
        if let Some(limit) = o.limit {
            let cap = limit.saturating_add(1);
            let counts = self.each(|_, run, q, _| count_up_to(run, q, cap))?;
            return Ok(counts.into_iter().sum::<u64>().min(cap));
        }
        Ok(self
            .each(|_, run, q, _| Ok(collect(run, q, &Count, None)? as u64))?
            .into_iter()
            .sum())
    }

    /// Tantivy aggregations (Elasticsearch's JSON) over the matches.
    pub fn aggregate(&self, aggs_json: &str) -> Result<String> {
        let o = &self.options;
        ensure!(
            o.offset == 0
                && o.collapse.is_none()
                && o.limit.is_none()
                && o.distinct.is_none()
                && o.sort.is_none()
                && o.highlight.is_none(),
            "offset, limit, collapse, distinct, sort and highlight are not options of tantivy_aggregate"
        );
        let aggs: Aggregations = serde_json::from_str(aggs_json).context("tantivy aggregations")?;
        let limits = AggregationLimitsGuard::default();
        let results = self.each(|i, run, q, _| {
            let context =
                AggContextParams::new(limits.clone(), self.splits[i].index.tokenizers().clone());
            collect(
                run,
                q,
                &DistributedAggregationCollector::from_aggs(aggs.clone(), context),
                None,
            )
        })?;
        let mut merged = IntermediateAggregationResults::default();
        for result in results {
            merged.merge_fruits(result)?;
        }
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

/// The snippets of a stored document, as a JSON object of HTML by field; a
/// field the query does not match is left out.
fn highlight_json(
    generators: &[(String, SnippetGenerator)],
    stored: &TantivyDocument,
) -> Result<String> {
    let mut out = Map::new();
    for (name, generator) in generators {
        let snippet = generator.snippet_from_doc(stored);
        if !snippet.is_empty() {
            out.insert(name.clone(), Value::from(snippet.to_html()));
        }
    }
    Ok(serde_json::to_string(&out)?)
}

/// A segment's open fast fields, by name.
type FastColumns = Vec<(String, Vec<DynamicColumn>)>;

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

/// A value of a single-valued fast field, comparable across segments and
/// splits.
#[derive(Clone, PartialEq, Eq, Hash)]
enum Key {
    Missing,
    I64(i64),
    U64(u64),
    F64(u64),
    Bool(bool),
    Date(i64),
    Str(Box<str>),
}

/// A fast field read as one key per document: by a raw number within a
/// segment (a string's term ordinal), resolved to a `Key` only when needed.
enum KeyColumn {
    Str(StrColumn),
    I64(Column<i64>),
    U64(Column<u64>),
    F64(Column<f64>),
    Bool(Column<bool>),
    Date(Column<DateTime>),
}

impl KeyColumn {
    fn open(reader: &SegmentReader, name: &str) -> Result<KeyColumn> {
        let mut handles = reader.fast_fields().dynamic_column_handles(name)?;
        ensure!(
            handles.len() == 1,
            "{name:?} is not a fast field of one type"
        );
        let (column, cardinality) = match handles.pop().unwrap().open()? {
            DynamicColumn::Str(c) => (KeyColumn::Str(c.clone()), c.ords().get_cardinality()),
            DynamicColumn::I64(c) => (KeyColumn::I64(c.clone()), c.get_cardinality()),
            DynamicColumn::U64(c) => (KeyColumn::U64(c.clone()), c.get_cardinality()),
            DynamicColumn::F64(c) => (KeyColumn::F64(c.clone()), c.get_cardinality()),
            DynamicColumn::Bool(c) => (KeyColumn::Bool(c.clone()), c.get_cardinality()),
            DynamicColumn::DateTime(c) => (KeyColumn::Date(c.clone()), c.get_cardinality()),
            _ => bail!("{name:?}: only text, numbers, booleans and dates can be grouped"),
        };
        ensure!(
            cardinality != Cardinality::Multivalued,
            "{name:?} has several values on a document"
        );
        Ok(column)
    }

    fn raw(&self, doc: DocId) -> Option<u64> {
        match self {
            KeyColumn::Str(c) => c.ords().first(doc),
            KeyColumn::I64(c) => c.first(doc).map(i64_to_u64),
            KeyColumn::U64(c) => c.first(doc),
            KeyColumn::F64(c) => c.first(doc).map(f64_to_u64),
            KeyColumn::Bool(c) => c.first(doc).map(u64::from),
            KeyColumn::Date(c) => c.first(doc).map(|d| i64_to_u64(d.into_timestamp_nanos())),
        }
    }

    /// Keys for distinct raw values: text by one pass over the dictionary's
    /// blocks, not a block read for each.
    fn keys(&self, raws: &[Option<u64>]) -> Vec<Key> {
        let KeyColumn::Str(column) = self else {
            return raws.iter().map(|&raw| self.key(raw)).collect();
        };
        let mut order: Vec<usize> = (0..raws.len()).filter(|&i| raws[i].is_some()).collect();
        order.sort_unstable_by_key(|&i| raws[i]);
        let mut keys = vec![Key::Missing; raws.len()];
        let mut at = order.iter();
        column
            .dictionary()
            .sorted_ords_to_term_cb(order.iter().map(|&i| raws[i].unwrap()), |term| {
                keys[*at.next().unwrap()] = Key::Str(String::from_utf8_lossy(term).into());
                Ok(())
            })
            .expect("term ordinals of the column's own dictionary");
        keys
    }

    fn key(&self, raw: Option<u64>) -> Key {
        let Some(raw) = raw else {
            return Key::Missing;
        };
        match self {
            KeyColumn::Str(c) => {
                let mut s = String::new();
                c.ord_to_str(raw, &mut s)
                    .expect("a term ordinal of the column's own dictionary");
                Key::Str(s.into())
            }
            KeyColumn::I64(_) => Key::I64(u64_to_i64(raw)),
            KeyColumn::U64(_) => Key::U64(raw),
            KeyColumn::F64(_) => Key::F64(raw),
            KeyColumn::Bool(_) => Key::Bool(raw != 0),
            KeyColumn::Date(_) => Key::Date(u64_to_i64(raw)),
        }
    }
}

/// The best hit of each distinct value of a fast field, at most `groups` of
/// them, best first. A document without a value is in a group of its own.
struct Collapse {
    field: String,
    groups: usize,
}

struct SegmentCollapse {
    segment: SegmentOrdinal,
    column: KeyColumn,
    groups: usize,
    best: HashMap<Option<u64>, (Score, DocId)>,
}

impl Collector for Collapse {
    type Fruit = Vec<(Score, Key, DocAddress)>;
    type Child = SegmentCollapse;

    fn for_segment(
        &self,
        segment: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> tantivy::Result<SegmentCollapse> {
        Ok(SegmentCollapse {
            segment,
            column: KeyColumn::open(reader, &self.field)
                .map_err(|e| tantivy::TantivyError::InvalidArgument(format!("{e:#}")))?,
            groups: self.groups,
            best: HashMap::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        let mut best: HashMap<Key, (Score, DocAddress)> = HashMap::new();
        for (score, key, addr) in fruits.into_iter().flatten() {
            match best.get(&key) {
                Some(&(kept, _)) if kept >= score => {}
                _ => {
                    best.insert(key, (score, addr));
                }
            }
        }
        let mut groups: Vec<_> = best
            .into_iter()
            .map(|(key, (score, addr))| (score, key, addr))
            .collect();
        groups.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.2.cmp(&b.2)));
        groups.truncate(self.groups);
        Ok(groups)
    }
}

impl SegmentCollector for SegmentCollapse {
    type Fruit = Vec<(Score, Key, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        let entry = self.best.entry(self.column.raw(doc));
        entry
            .and_modify(|best| {
                if score > best.0 {
                    *best = (score, doc);
                }
            })
            .or_insert((score, doc));
    }

    fn harvest(self) -> Self::Fruit {
        let mut groups: Vec<_> = self.best.into_iter().collect();
        if groups.len() > self.groups {
            // Best first, and the lowest document among equals, as in TopDocs.
            groups.select_nth_unstable_by(self.groups - 1, |a, b| {
                b.1 .0.total_cmp(&a.1 .0).then(a.1 .1.cmp(&b.1 .1))
            });
            groups.truncate(self.groups);
        }
        let keys = self
            .column
            .keys(&groups.iter().map(|g| g.0).collect::<Vec<_>>());
        groups
            .into_iter()
            .zip(keys)
            .map(|((_, (score, doc)), key)| (score, key, DocAddress::new(self.segment, doc)))
            .collect()
    }
}

/// The distinct values of a fast field among the matches (not the missing).
struct Distinct {
    field: String,
}

struct SegmentDistinct {
    column: KeyColumn,
    seen: HashSet<u64>,
}

impl Collector for Distinct {
    type Fruit = HashSet<Key>;
    type Child = SegmentDistinct;

    fn for_segment(
        &self,
        _: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> tantivy::Result<SegmentDistinct> {
        Ok(SegmentDistinct {
            column: KeyColumn::open(reader, &self.field)
                .map_err(|e| tantivy::TantivyError::InvalidArgument(format!("{e:#}")))?,
            seen: HashSet::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        Ok(fruits.into_iter().flatten().collect())
    }
}

impl SegmentCollector for SegmentDistinct {
    type Fruit = HashSet<Key>;

    fn collect(&mut self, doc: DocId, _: Score) {
        if let Some(raw) = self.column.raw(doc) {
            self.seen.insert(raw);
        }
    }

    fn harvest(self) -> Self::Fruit {
        let raws: Vec<Option<u64>> = self.seen.into_iter().map(Some).collect();
        self.column.keys(&raws).into_iter().collect()
    }
}

/// Matches documents whose fast field value is in a set, by scanning the
/// field: for deleting excluded documents in a merge, which reads every
/// document anyway.
#[derive(Clone, Debug)]
pub(crate) struct InSetQuery {
    field: String,
    set: Arc<RoaringTreemap>,
}

impl InSetQuery {
    pub(crate) fn new(field: String, exclude: &Exclude) -> InSetQuery {
        InSetQuery {
            field,
            set: exclude.set.clone(),
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
            .into_iter()
            .map(|h| (h.split, h.score, h.doc))
            .collect()
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
        assert!(Request::new(s, "x", r#"{"nope": 1}"#, None).is_err());
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
            assert_eq!(Exclude::from_roaring(&bytes).unwrap().set.len(), 0);
        }
        let set: RoaringTreemap = (0..12346u64).map(|i| i << 32).collect();
        let mut bytes = Vec::new();
        set.serialize_into(&mut bytes).unwrap();
        assert_eq!(*Exclude::from_roaring(&bytes).unwrap().set, set);
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

#[cfg(test)]
mod feature_tests {
    use super::*;
    use crate::split::testing::{build_with, open, split, OPTIONS};

    const GROUPED: &str = r#"[
        {"name": "id", "type": "i64", "options": {"stored": true, "indexed": true, "fast": true}},
        {"name": "body", "type": "text", "options": {"stored": false,
            "indexing": {"record": "position", "fieldnorms": true, "tokenizer": "fts"}}},
        {"name": "group", "type": "text", "options": {"stored": true, "fast": true,
            "indexing": {"record": "basic", "tokenizer": "raw"}}}
    ]"#;

    const GROUP_DOCS: &[&str] = &[
        r#"{"id": 1, "body": "fox", "group": "a"}"#,
        r#"{"id": 2, "body": "fox fox fox", "group": "a"}"#,
        r#"{"id": 3, "body": "fox", "group": "b"}"#,
        r#"{"id": 4, "body": "fox fox", "group": "c"}"#,
        r#"{"id": 5, "body": "fox"}"#,
        r#"{"id": 6, "body": "cat", "group": "d"}"#,
    ];

    fn grouped(docs: &[&str]) -> Split {
        open(build_with(GROUPED, OPTIONS, docs))
    }

    /// (id, group) of each hit, best first.
    fn found(
        splits: &[&Split],
        query: &str,
        options: &str,
        ex: Option<&Exclude>,
    ) -> Vec<(i64, Option<String>)> {
        Request::new(splits, query, options, ex)
            .unwrap()
            .hits()
            .unwrap()
            .into_iter()
            .map(|h| {
                let doc: Value = serde_json::from_str(&h.doc).unwrap();
                (
                    doc["id"].as_i64().unwrap(),
                    doc.get("group").map(|g| g.as_str().unwrap().to_owned()),
                )
            })
            .collect()
    }

    const FAST: &str = r#""fast": ["id", "group"]"#;

    #[test]
    fn collapses_to_the_best_hit_of_each_value() {
        let s = grouped(GROUP_DOCS);
        let all = found(&[&s], "fox", &format!("{{{FAST}}}"), None);
        assert_eq!(all.len(), 5);
        // The best of each group is its first hit in the ranking.
        let mut best: Vec<(i64, Option<String>)> = Vec::new();
        for hit in &all {
            if !best.iter().any(|b| b.1 == hit.1) {
                best.push(hit.clone());
            }
        }
        assert_eq!(best.len(), 4); // a, b, c and the one without a group
        let collapsed = found(
            &[&s],
            "fox",
            &format!(r#"{{"collapse": "group", {FAST}}}"#),
            None,
        );
        assert_eq!(collapsed, best);
        // top_k and offset count groups.
        let page = |opts: &str| {
            found(
                &[&s],
                "fox",
                &format!(r#"{{"collapse": "group", {FAST}, {opts}}}"#),
                None,
            )
        };
        assert_eq!(page(r#""top_k": 2"#), best[..2]);
        assert_eq!(page(r#""top_k": 2, "offset": 1"#), best[1..3]);
        assert_eq!(page(r#""offset": 3"#), best[3..]);
        assert!(page(r#""top_k": 0"#).is_empty());
        // Over splits, a value's best hit wins, wherever it is.
        let (a, b) = (grouped(&GROUP_DOCS[..3]), grouped(&GROUP_DOCS[3..]));
        let split_up = found(
            &[&a, &b],
            "fox",
            &format!(r#"{{"collapse": "group", {FAST}}}"#),
            None,
        );
        let ids = |v: &[(i64, Option<String>)]| v.iter().map(|h| h.0).collect::<HashSet<_>>();
        assert_eq!(ids(&split_up), ids(&best));
        // Excluded documents are not in a group: the next best takes over.
        let ex = Exclude::from_ids([2]);
        let opts = format!(r#"{{"collapse": "group", {FAST}, "exclude_field": "id"}}"#);
        let without = found(&[&s], "fox", &opts, Some(&ex));
        assert!(without.contains(&(1, Some("a".into()))) && !without.iter().any(|h| h.0 == 2));
        // Groups need a single-valued fast field.
        let collapse = |field: &str| {
            Request::new(&[&s], "fox", &format!(r#"{{"collapse": "{field}"}}"#), None)
                .unwrap()
                .hits()
        };
        assert!(collapse("body").is_err() && collapse("nosuch").is_err());
        let multi = split(&[r#"{"id": 1, "body": "fox", "tag": ["x", "y"]}"#]);
        let err = Request::new(&[&multi], "fox", r#"{"collapse": "tag"}"#, None)
            .unwrap()
            .hits()
            .unwrap_err();
        assert!(format!("{err:#}").contains("several values"), "{err:#}");
    }

    #[test]
    fn counts_distinct_values_exactly() {
        let (a, b) = (grouped(&GROUP_DOCS[..3]), grouped(&GROUP_DOCS[2..]));
        let distinct = |splits: &[&Split], query: &str, field: &str, ex: Option<&Exclude>| {
            let options = format!(r#"{{"distinct": "{field}", "exclude_field": "id"}}"#);
            Request::new(splits, query, &options, ex).unwrap().count()
        };
        // a, b, c: documents without a value, and the unmatched d, are not values.
        assert_eq!(distinct(&[&a, &b], "fox", "group", None).unwrap(), 3);
        assert_eq!(distinct(&[&a, &b], "*", "group", None).unwrap(), 4);
        // Document 3 is in both splits and is the only one of group b.
        assert_eq!(
            distinct(&[&a, &b], "fox", "group", Some(&Exclude::from_ids([3]))).unwrap(),
            2
        );
        assert_eq!(distinct(&[&a, &b], "fox", "id", None).unwrap(), 5);
        assert_eq!(distinct(&[&a], "nomatch", "group", None).unwrap(), 0);
        assert!(distinct(&[&a], "fox", "body", None).is_err());
        let only_a = [&a];
        let combined =
            Request::new(&only_a, "fox", r#"{"distinct": "id", "limit": 3}"#, None).unwrap();
        assert!(combined.count().is_err());
        assert!(Request::new(&[&a], "fox", r#"{"distinct": "id"}"#, None)
            .unwrap()
            .hits()
            .is_err());
    }

    #[test]
    fn caps_counts_with_a_limit() {
        let s = grouped(GROUP_DOCS);
        let (a, b) = (grouped(&GROUP_DOCS[..3]), grouped(&GROUP_DOCS[3..]));
        let count = |splits: &[&Split], limit: Option<u64>, ex: Option<&Exclude>| {
            let options = match limit {
                Some(n) => format!(r#"{{"limit": {n}, "exclude_field": "id"}}"#),
                None => r#"{"exclude_field": "id"}"#.to_owned(),
            };
            Request::new(splits, "fox", &options, ex)
                .unwrap()
                .count()
                .unwrap()
        };
        assert_eq!(count(&[&s], None, None), 5);
        // The count, or one more than the limit if there are more matches.
        for (limit, want) in [(0, 1), (1, 2), (4, 5), (5, 5), (6, 5), (u64::MAX, 5)] {
            assert_eq!(count(&[&s], Some(limit), None), want, "limit {limit}");
        }
        assert_eq!(count(&[&a, &b], Some(2), None), 3);
        assert_eq!(count(&[&a, &b], Some(9), None), 5);
        let dead = Exclude::from_ids([1, 2, 3]);
        assert_eq!(count(&[&s], Some(2), Some(&dead)), 2);
        assert_eq!(count(&[&s], Some(3), Some(&dead)), 2);
        assert!(Request::new(&[&s], "fox", r#"{"limit": 1}"#, None)
            .unwrap()
            .hits()
            .is_err());
        assert!(Request::new(&[&s], "fox", r#"{"limit": 1}"#, None)
            .unwrap()
            .aggregate("{}")
            .is_err());
        let only_s = [&s];
        let offset = Request::new(&only_s, "fox", r#"{"offset": 1}"#, None).unwrap();
        assert!(offset.count().is_err() && offset.aggregate("{}").is_err());
    }

    #[test]
    fn pages_with_an_offset() {
        let s = grouped(GROUP_DOCS);
        let (a, b) = (grouped(&GROUP_DOCS[..3]), grouped(&GROUP_DOCS[3..]));
        for splits in [&[&s][..], &[&a, &b][..]] {
            let all = found(splits, "fox", &format!("{{{FAST}}}"), None);
            for (offset, size) in [(0, 2), (1, 2), (3, 2), (4, 3), (5, 1), (9, 2)] {
                let options = format!(r#"{{{FAST}, "top_k": {size}, "offset": {offset}}}"#);
                let page = found(splits, "fox", &options, None);
                let want: Vec<_> = all.iter().skip(offset).take(size).cloned().collect();
                assert_eq!(page, want, "offset {offset} top_k {size}");
            }
            let options = format!(r#"{{{FAST}, "offset": 2}}"#);
            assert_eq!(found(splits, "fox", &options, None), all[2..]);
        }
        let huge = format!(r#"{{{FAST}, "top_k": 2, "offset": {}}}"#, usize::MAX);
        assert!(found(&[&s], "fox", &huge, None).is_empty());
    }

    #[test]
    fn searches_splits_in_parallel_as_it_does_serially() {
        let docs: Vec<String> = (0..60)
            .map(|i| {
                format!(
                    r#"{{"id": {i}, "body": "{} cat", "group": "g{}"}}"#,
                    "fox ".repeat(i % 4),
                    i % 7
                )
            })
            .collect();
        let splits: Vec<Split> = docs
            .chunks(10)
            .map(|chunk| grouped(&chunk.iter().map(String::as_str).collect::<Vec<_>>()))
            .collect();
        let refs: Vec<&Split> = splits.iter().collect();
        let ex = Exclude::from_ids((0..60).filter(|i| i % 5 == 0));
        let request = |threads: usize, options: &str| {
            Request::new(&refs, "fox", options, Some(&ex))
                .unwrap()
                .with_threads(threads)
        };
        for options in [
            r#"{"exclude_field": "id", "fast": ["id"]}"#,
            r#"{"exclude_field": "id", "fast": ["id"], "top_k": 7, "offset": 2}"#,
            r#"{"exclude_field": "id", "fast": ["id", "group"], "collapse": "group", "top_k": 4}"#,
            r#"{"exclude_field": "id", "fast": ["id"], "global_stats": true, "top_k": 9}"#,
        ] {
            let serial = request(1, options).hits().unwrap();
            for threads in [2, 4, 64] {
                assert_eq!(
                    request(threads, options).hits().unwrap(),
                    serial,
                    "{options} on {threads}"
                );
            }
        }
        let count = |threads, options| request(threads, options).count().unwrap();
        let by = r#"{"exclude_field": "id"}"#;
        assert_eq!(count(1, by), count(8, by));
        let distinct = r#"{"exclude_field": "id", "distinct": "group"}"#;
        assert_eq!(count(1, distinct), count(8, distinct));
        let aggs = r#"{"g": {"terms": {"field": "group", "size": 20}}}"#;
        let aggregate = |threads| request(threads, by).aggregate(aggs).unwrap();
        assert_eq!(aggregate(1), aggregate(8));
        // An error in any split is the call's error.
        let missing = split(&[r#"{"body": "fox"}"#]);
        let mixed = [&splits[0], &missing, &splits[1]];
        let err = Request::new(&mixed, "fox", by, Some(&ex))
            .unwrap()
            .with_threads(4)
            .count();
        assert!(err.is_err());
    }

    #[test]
    fn keeps_alive_sets_per_split_and_exclusion() {
        let s = grouped(GROUP_DOCS);
        let cached = || s.alive.0.lock().unwrap().len();
        let one = Exclude::from_ids([1, 2]);
        let first = s.alive.get(&s, "id", &one).unwrap();
        assert!(Arc::ptr_eq(&first, &s.alive.get(&s, "id", &one).unwrap()));
        assert_eq!(cached(), 1);
        // The same ids, however they came, are the same exclusion.
        assert!(Arc::ptr_eq(
            &first,
            &s.alive.get(&s, "id", &Exclude::from_ids([1, 2])).unwrap()
        ));
        let other = Exclude::from_ids([3]);
        assert!(!Arc::ptr_eq(
            &first,
            &s.alive.get(&s, "id", &other).unwrap()
        ));
        assert_eq!(cached(), 2);
        // A failure is reported, and not kept.
        assert!(s.alive.get(&s, "body", &one).is_err());
        assert!(s.alive.get(&s, "nosuch", &one).is_err());
        assert_eq!(cached(), 2);
        // The most recent few are kept.
        for i in 10..20 {
            s.alive.get(&s, "id", &Exclude::from_ids([i])).unwrap();
        }
        assert_eq!(cached(), ALIVE_SETS_PER_SPLIT);
        // Dead documents do not count, or crowd out the living, or score.
        let options = r#"{"exclude_field": "id", "top_k": 1, "fast": ["id"]}"#;
        let best = |ex: &Exclude| found(&[&s], "fox", options, Some(ex))[0].0;
        assert_eq!(best(&Exclude::from_ids([])), 2);
        assert_eq!(best(&one), 4);
        assert_eq!(best(&Exclude::from_ids([2, 4])), 1);
        // 32-bit ids are not 64-bit ones: a bitmap and ids agree.
        let mut bytes = Vec::new();
        RoaringBitmap::from_iter([2u32, 4])
            .serialize_into(&mut bytes)
            .unwrap();
        assert_eq!(best(&Exclude::from_roaring(&bytes).unwrap()), 1);
    }
}

#[cfg(test)]
mod sort_and_highlight_tests {
    use super::*;
    use crate::split::testing::{build_with, open};
    use tantivy::schema::{FAST, INDEXED, STORED, STRING, TEXT};

    fn schema(date: bool) -> String {
        let mut b = Schema::builder();
        b.add_i64_field("id", FAST | STORED | INDEXED);
        b.add_f64_field("rank", FAST | STORED);
        b.add_u64_field("n", FAST | STORED);
        if date {
            b.add_date_field("seen", FAST | STORED);
        } else {
            b.add_text_field("seen", STRING | STORED); // not a fast field, nor a date
        }
        b.add_text_field("name", STRING | FAST | STORED);
        b.add_text_field("body", TEXT | STORED);
        serde_json::to_string(&b.build()).unwrap()
    }

    const DOCS: &[&str] = &[
        r#"{"id": 1, "rank": 2.5, "n": 30, "seen": "2026-01-03T00:00:00Z", "name": "carol", "body": "roof tile"}"#,
        r#"{"id": 2, "rank": -1.0, "n": 10, "seen": "2026-01-01T00:00:00Z", "name": "alice", "body": "a <b> & roof"}"#,
        r#"{"id": 3, "rank": 9.0, "n": 20, "seen": "2026-01-02T00:00:00Z", "name": "bob", "body": "roof slate roof"}"#,
        r#"{"id": 4, "body": "roof"}"#,
        r#"{"id": 5, "rank": 2.5, "n": 30, "seen": "2026-01-03T00:00:00Z", "name": "carol", "body": "roof"}"#,
        r#"{"id": 6, "rank": 0.0, "n": 5, "seen": "2025-12-31T00:00:00Z", "name": "dave", "body": "cat"}"#,
    ];

    fn splits(parts: &[&[&str]]) -> Vec<Split> {
        parts
            .iter()
            .map(|docs| open(build_with(&schema(true), "", docs)))
            .collect()
    }

    fn search(splits: &[Split], options: &str) -> Result<Vec<Hit>> {
        let refs: Vec<&Split> = splits.iter().collect();
        Request::new(&refs, "roof", options, None)?.hits()
    }

    fn ids(hits: &[Hit]) -> Vec<i64> {
        hits.iter()
            .map(|h| {
                serde_json::from_str::<Value>(&h.doc).unwrap()["id"]
                    .as_i64()
                    .unwrap()
            })
            .collect()
    }

    /// `ids` are in this order, the ids of a group in any order among themselves.
    fn assert_order(ids: &[i64], groups: &[&[i64]]) {
        let mut at = 0;
        for group in groups {
            let mut got = ids[at..at + group.len()].to_vec();
            let mut want = group.to_vec();
            got.sort();
            want.sort();
            assert_eq!(got, want, "in {ids:?}, expected {groups:?}");
            at += group.len();
        }
        assert_eq!(at, ids.len(), "in {ids:?}, expected {groups:?}");
    }

    #[test]
    fn sorts_by_a_fast_field_with_missing_values_last() {
        for parts in [&[DOCS][..], &[&DOCS[..3], &DOCS[3..]][..]] {
            let s = splits(parts);
            let sorted = |field: &str, order: &str| {
                let options = format!(
                    r#"{{"sort": {{"field": "{field}", "order": "{order}"}}, "fast": ["id"]}}"#
                );
                let hits = search(&s, &options).unwrap();
                assert!(
                    hits.iter().all(|h| h.score.is_nan()),
                    "a sorted hit has no score"
                );
                ids(&hits)
            };
            assert_order(&sorted("rank", "asc"), &[&[2], &[1, 5], &[3], &[4]]);
            assert_order(&sorted("rank", "desc"), &[&[3], &[1, 5], &[2], &[4]]);
            assert_order(&sorted("n", "asc"), &[&[2], &[3], &[1, 5], &[4]]);
            assert_order(&sorted("seen", "desc"), &[&[1, 5], &[3], &[2], &[4]]);
            assert_order(&sorted("name", "asc"), &[&[2], &[3], &[1, 5], &[4]]);
            assert_order(&sorted("name", "desc"), &[&[1, 5], &[3], &[2], &[4]]);
            assert_order(&sorted("id", "desc"), &[&[5], &[4], &[3], &[2], &[1]]);
            // The field alone is ascending; unsorted hits have scores.
            let shorthand = search(&s, r#"{"sort": "id", "fast": ["id"]}"#).unwrap();
            assert_eq!(ids(&shorthand), [1, 2, 3, 4, 5]);
            assert!(search(&s, r#"{"fast": ["id"]}"#)
                .unwrap()
                .iter()
                .all(|h| !h.score.is_nan()));
            // Pages cut the one ordering, wherever the splits are.
            let all = sorted("n", "desc");
            for (offset, size) in [(0, 2), (1, 3), (3, 5), (9, 2)] {
                let options = format!(
                    r#"{{"sort": {{"field": "n", "order": "desc"}}, "fast": ["id"], "top_k": {size}, "offset": {offset}}}"#
                );
                let page = ids(&search(&s, &options).unwrap());
                assert_eq!(
                    page,
                    all.iter()
                        .copied()
                        .skip(offset)
                        .take(size)
                        .collect::<Vec<_>>()
                );
            }
            // The same call, the same hits.
            assert_eq!(sorted("n", "desc"), all);
        }
    }

    #[test]
    fn sorts_what_is_not_excluded() {
        let s = splits(&[DOCS]);
        let refs: Vec<&Split> = s.iter().collect();
        let ex = Exclude::from_ids([3, 4]);
        let options = r#"{"sort": {"field": "rank", "order": "desc"}, "fast": ["id"], "exclude_field": "id", "top_k": 2}"#;
        let hits = Request::new(&refs, "roof", options, Some(&ex))
            .unwrap()
            .hits()
            .unwrap();
        assert_order(&ids(&hits), &[&[1, 5]]); // 3 and 4 are gone; 2 is next but cut
    }

    #[test]
    fn rejects_sorts_it_cannot_do() {
        let s = splits(&[DOCS]);
        let err = |options: &str| format!("{:#}", search(&s, options).expect_err("should fail"));
        assert!(err(r#"{"sort": "body"}"#).contains("not a fast field"));
        assert!(err(r#"{"sort": "nosuch"}"#).contains("no field"));
        assert!(err(r#"{"sort": {"field": "id", "order": "up"}}"#).contains("did not match"));
        assert!(err(r#"{"sort": {"field": "id", "extra": 1}}"#).contains("did not match"));
        assert!(err(r#"{"sort": "id", "collapse": "name"}"#).contains("cannot be combined"));
        // Splits must agree on the field's type.
        let other = open(build_with(
            &schema(false),
            "",
            &[r#"{"id": 9, "body": "roof", "seen": "x"}"#],
        ));
        let mixed = [&s[0], &other];
        let refs: Vec<&Split> = mixed.to_vec();
        let e = Request::new(&refs, "roof", r#"{"sort": "seen"}"#, None)
            .err()
            .expect("should fail");
        assert!(format!("{e:#}").contains("not a fast field"), "{e:#}");
        let i64s = open(build_with(
            &schema(true),
            "",
            &[r#"{"id": 9, "body": "roof"}"#],
        ));
        let mut strings = Schema::builder();
        strings.add_i64_field("id", FAST | STORED | INDEXED);
        strings.add_text_field("n", STRING | FAST | STORED);
        strings.add_text_field("body", TEXT | STORED);
        let strings = open(build_with(
            &serde_json::to_string(&strings.build()).unwrap(),
            "",
            &[r#"{"id": 8, "n": "x", "body": "roof"}"#],
        ));
        let refs = [&i64s, &strings];
        let e = Request::new(&refs, "roof", r#"{"sort": "n"}"#, None)
            .err()
            .expect("should fail");
        assert!(format!("{e:#}").contains("different types"), "{e:#}");
        // A count or an aggregation has no order.
        let one: Vec<&Split> = s.iter().collect();
        let sorted = Request::new(&one, "roof", r#"{"sort": "id"}"#, None).unwrap();
        assert!(sorted.count().is_err() && sorted.aggregate("{}").is_err());
    }

    #[test]
    fn highlights_stored_text_where_the_query_matches() {
        let s = splits(&[&DOCS[..3], &DOCS[3..]]);
        let hits = search(
            &s,
            r#"{"fast": ["id"], "highlight": {"fields": ["body", "name"]}}"#,
        )
        .unwrap();
        assert!(hits.iter().all(|h| h.highlight.is_some()));
        let by_id: HashMap<i64, Value> = ids(&hits)
            .into_iter()
            .zip(&hits)
            .map(|(id, h)| {
                (
                    id,
                    serde_json::from_str(h.highlight.as_ref().unwrap()).unwrap(),
                )
            })
            .collect();
        assert_eq!(by_id[&1]["body"], "<b>roof</b> tile");
        assert_eq!(by_id[&3]["body"], "<b>roof</b> slate <b>roof</b>");
        // The text is escaped; only the match is marked; `name` has no match.
        assert_eq!(by_id[&2]["body"], "a &lt;b&gt; &amp; <b>roof</b>");
        assert!(by_id.values().all(|h| h.get("name").is_none()));
        // The document still comes from the fast fields, not the highlight's read.
        assert_eq!(hits[0].doc.matches("body").count(), 0);
        // A snippet is no longer than asked.
        let long = format!(r#"{{"id": 7, "body": "roof {}"}}"#, "filler ".repeat(100));
        let s = splits(&[&[long.as_str()]]);
        let hits = search(
            &s,
            r#"{"fast": ["id"], "highlight": {"fields": ["body"], "max_chars": 40}}"#,
        )
        .unwrap();
        let snippet: Value = serde_json::from_str(hits[0].highlight.as_ref().unwrap()).unwrap();
        assert!(snippet["body"].as_str().unwrap().len() < 80, "{snippet}");
        // Without it, no snippets.
        assert!(search(&s, "{}")
            .unwrap()
            .iter()
            .all(|h| h.highlight.is_none()));
    }

    #[test]
    fn rejects_highlights_it_cannot_do() {
        let s = splits(&[DOCS]);
        let err = |options: &str| format!("{:#}", search(&s, options).expect_err("should fail"));
        assert!(err(r#"{"highlight": {"fields": []}}"#).contains("needs fields"));
        assert!(err(r#"{"highlight": {"fields": ["nosuch"]}}"#).contains("no field"));
        assert!(err(r#"{"highlight": {"fields": ["id"]}}"#).contains("not a stored text field"));
        assert!(
            err(r#"{"highlight": {"fields": ["body"], "pre": "<em>"}}"#).contains("unknown field")
        );
        let one: Vec<&Split> = s.iter().collect();
        let request =
            Request::new(&one, "roof", r#"{"highlight": {"fields": ["body"]}}"#, None).unwrap();
        assert!(request.count().is_err() && request.aggregate("{}").is_err());
        // An unstored text field cannot be shown.
        let mut b = Schema::builder();
        b.add_text_field("body", TEXT);
        let unstored = open(build_with(
            &serde_json::to_string(&b.build()).unwrap(),
            "",
            &[r#"{"body": "roof"}"#],
        ));
        let refs = [&unstored];
        let e = Request::new(
            &refs,
            "roof",
            r#"{"highlight": {"fields": ["body"]}}"#,
            None,
        )
        .err()
        .expect("should fail");
        assert!(format!("{e:#}").contains("not a stored text field"));
    }
}

#[cfg(test)]
mod properties;
