//! Tantivy splits. A split is one immutable tantivy index, bundled into a
//! single file: built with tantivy's own directory in a local temporary one,
//! then streamed out; searched through a byte-range reader. Where the file
//! lives (any DuckDB filesystem), and which splits make up an index, are the
//! caller's concern.
//!
//! Tantivy's own formats pass straight through as JSON: the schema, documents,
//! index settings and the query language. Build options may also name
//! tokenizers (`Analyzer`); they ride in the commit payload so every reader
//! registers the same ones.
//!
//! Bundle: the files' bytes back to back, a JSON footer naming each file's
//! [offset, length], the footer's length (u64, little-endian) and `MAGIC`.

use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context as _, Result};
use serde::{Deserialize, Serialize};
use tantivy::collector::{Collector, SegmentCollector, TopDocs};
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    DirectoryLock, FileHandle, Lock, OwnedBytes, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::merge_policy::NoMergePolicy;
use tantivy::query::QueryParser;
use tantivy::schema::{FieldType, Schema};
use tantivy::tokenizer::{
    AlphaNumOnlyFilter, AsciiFoldingFilter, Language, LowerCaser, NgramTokenizer, RawTokenizer,
    RegexTokenizer, RemoveLongFilter, SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer,
    WhitespaceTokenizer,
};
use tantivy::{
    DocAddress, DocId, Document, HasLen, Index, IndexReader, IndexSettings, IndexWriter,
    ReloadPolicy, Score, SegmentOrdinal, SegmentReader, TantivyDocument,
};

const MAGIC: &[u8; 8] = b"tantivy1";

/// Build options, as JSON: tokenizers to register by name, tantivy
/// `IndexSettings`, the writer's memory budget in bytes, and whether to merge
/// the new index into one segment (one read per term per query).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BuildOptions {
    #[serde(default)]
    tokenizers: BTreeMap<String, Analyzer>,
    #[serde(default)]
    settings: IndexSettings,
    #[serde(default = "default_memory_budget")]
    memory_budget: usize,
    #[serde(default = "yes")]
    merge: bool,
}

fn default_memory_budget() -> usize {
    256 << 20
}

fn yes() -> bool {
    true
}

/// Search options, as JSON: `top_k` (default: every hit), the default
/// `fields` for terms that name none (default: every indexed text field),
/// `conjunctive` (all terms must match) and `strict` (fail on query syntax
/// errors instead of dropping what tantivy cannot parse).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchOptions {
    top_k: Option<usize>,
    fields: Option<Vec<String>>,
    #[serde(default)]
    conjunctive: bool,
    #[serde(default)]
    strict: bool,
}

fn options<'a, T: Deserialize<'a>>(json: &'a str, what: &str) -> Result<T> {
    let json = if json.trim().is_empty() { "{}" } else { json };
    serde_json::from_str(json).with_context(|| format!("tantivy {what} options"))
}

/// A tokenizer and its filters, built into a tantivy `TextAnalyzer`:
/// `{"tokenizer": "simple", "filters": ["lowercase", {"stemmer": "english"}]}`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Analyzer {
    #[serde(default)]
    tokenizer: TokenizerSpec,
    #[serde(default)]
    filters: Vec<FilterSpec>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum TokenizerSpec {
    #[default]
    Simple,
    Whitespace,
    Raw,
    Ngram {
        min: usize,
        max: usize,
        #[serde(default)]
        prefix_only: bool,
    },
    Regex(String),
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum FilterSpec {
    Lowercase,
    AsciiFolding,
    AlphaNumOnly,
    RemoveLong(usize),
    StopWords(StopWords),
    Stemmer(String),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StopWords {
    Language(String),
    Words(Vec<String>),
}

/// A tantivy language by its lowercase name; "porter" is English.
fn language(name: &str) -> Result<Language> {
    let lower = match name.to_ascii_lowercase() {
        n if n == "porter" => "english".to_owned(),
        n => n,
    };
    let title = lower[..1.min(lower.len())].to_ascii_uppercase() + &lower[1.min(lower.len())..];
    serde_json::from_value(serde_json::Value::String(title))
        .map_err(|_| anyhow!("tantivy has no language {name:?}"))
}

impl Analyzer {
    fn build(&self) -> Result<TextAnalyzer> {
        let mut b = match &self.tokenizer {
            TokenizerSpec::Simple => TextAnalyzer::builder(SimpleTokenizer::default()).dynamic(),
            TokenizerSpec::Whitespace => {
                TextAnalyzer::builder(WhitespaceTokenizer::default()).dynamic()
            }
            TokenizerSpec::Raw => TextAnalyzer::builder(RawTokenizer::default()).dynamic(),
            TokenizerSpec::Ngram {
                min,
                max,
                prefix_only,
            } => TextAnalyzer::builder(NgramTokenizer::new(*min, *max, *prefix_only)?).dynamic(),
            TokenizerSpec::Regex(pattern) => {
                TextAnalyzer::builder(RegexTokenizer::new(pattern)?).dynamic()
            }
        };
        for filter in &self.filters {
            b = match filter {
                FilterSpec::Lowercase => b.filter_dynamic(LowerCaser),
                FilterSpec::AsciiFolding => b.filter_dynamic(AsciiFoldingFilter),
                FilterSpec::AlphaNumOnly => b.filter_dynamic(AlphaNumOnlyFilter),
                FilterSpec::RemoveLong(n) => b.filter_dynamic(RemoveLongFilter::limit(*n)),
                FilterSpec::StopWords(StopWords::Language(l)) => b.filter_dynamic(
                    StopWordFilter::new(language(l)?)
                        .ok_or_else(|| anyhow!("tantivy has no stop words for {l:?}"))?,
                ),
                FilterSpec::StopWords(StopWords::Words(words)) => {
                    b.filter_dynamic(StopWordFilter::remove(words.clone()))
                }
                FilterSpec::Stemmer(l) => b.filter_dynamic(Stemmer::new(language(l)?)),
            };
        }
        Ok(b.build())
    }
}

/// Register the options' tokenizers, for text and fast fields alike.
fn register(index: &Index, options: &BuildOptions) -> Result<()> {
    for (name, spec) in &options.tokenizers {
        let analyzer = spec
            .build()
            .with_context(|| format!("tokenizer {name:?}"))?;
        index.tokenizers().register(name, analyzer.clone());
        index.fast_field_tokenizer().register(name, analyzer);
    }
    Ok(())
}

/// A split being built, in a local temporary directory (removed on drop).
pub struct Build {
    writer: IndexWriter,
    index: Index,
    schema: Schema,
    payload: String,
    merge: bool,
    docs: AtomicU64,
    // Last: removed once the writer's threads have stopped.
    dir: tempfile::TempDir,
}

impl Build {
    pub fn new(schema: &str, options_json: &str) -> Result<Build> {
        let schema: Schema = serde_json::from_str(schema).context("tantivy schema")?;
        let options: BuildOptions = options(options_json, "index")?;
        let dir = tempfile::Builder::new()
            .prefix("duckdb-tantivy-")
            .tempdir()?;
        let index = Index::builder()
            .schema(schema.clone())
            .settings(options.settings.clone())
            .create_in_dir(dir.path())?;
        register(&index, &options)?;
        let writer: IndexWriter = index.writer(options.memory_budget)?;
        if options.merge {
            writer.set_merge_policy(Box::new(NoMergePolicy));
        }
        Ok(Build {
            writer,
            index,
            schema,
            payload: if options_json.trim().is_empty() {
                "{}".into()
            } else {
                options_json.into()
            },
            merge: options.merge,
            docs: AtomicU64::new(0),
            dir,
        })
    }

    /// Add a document: a JSON object of field values (a value or an array of
    /// them), parsed as tantivy does; nulls and unknown fields are skipped.
    /// Any thread.
    pub fn add(&self, json: &str) -> Result<()> {
        let mut doc: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(json).context("a tantivy document is a JSON object")?;
        doc.retain(|_, v| !v.is_null());
        let doc = TantivyDocument::from_json_object(&self.schema, doc)?;
        self.writer.add_document(doc)?;
        self.docs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Commit, and write the split's bundle to `out`. Returns the documents.
    pub fn finish(self, mut out: impl FnMut(&[u8]) -> Result<()>) -> Result<u64> {
        let Build {
            mut writer,
            index,
            payload,
            merge,
            docs,
            dir,
            ..
        } = self;
        let mut commit = writer.prepare_commit()?;
        commit.set_payload(&payload);
        commit.commit()?;
        if merge {
            let segments = index.searchable_segment_ids()?;
            if segments.len() > 1 {
                writer.merge(&segments).wait()?;
            }
        }
        writer.wait_merging_threads()?;
        // Everything but lock files, in a stable order.
        let mut names = Vec::new();
        for entry in std::fs::read_dir(dir.path())? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow!("non-UTF-8 file name"))?;
            if entry.file_type()?.is_file() && !name.starts_with(".tantivy-") {
                names.push(name);
            }
        }
        names.sort();
        let mut footer = Footer::default();
        let mut offset = 0u64;
        let mut buf = vec![0; 1 << 20];
        for name in names {
            let mut file = std::fs::File::open(dir.path().join(&name))?;
            let start = offset;
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                out(&buf[..n])?;
                offset += n as u64;
            }
            footer.files.insert(name, [start, offset - start]);
        }
        let footer = serde_json::to_vec(&footer)?;
        out(&footer)?;
        out(&(footer.len() as u64).to_le_bytes())?;
        out(MAGIC)?;
        Ok(docs.load(Ordering::Relaxed))
    }
}

#[derive(Serialize, Deserialize, Default)]
struct Footer {
    /// name -> [offset, length]
    files: BTreeMap<String, [u64; 2]>,
}

/// Reads exactly `buf.len()` bytes of the split from an offset; any thread.
pub type ReadAt = dyn Fn(u64, &mut [u8]) -> io::Result<()> + Send + Sync;

/// A split open for search.
pub struct Split {
    index: Index,
    reader: IndexReader,
}

impl Split {
    pub fn open(size: u64, read: Arc<ReadAt>) -> Result<Split> {
        let not_a_split = || anyhow!("not a tantivy split ({size} bytes)");
        if size < 16 {
            return Err(not_a_split());
        }
        let mut tail = [0u8; 16];
        read(size - 16, &mut tail)?;
        if &tail[8..] != MAGIC {
            return Err(not_a_split());
        }
        let footer_len = u64::from_le_bytes(tail[..8].try_into()?);
        let data_len = (size - 16)
            .checked_sub(footer_len)
            .ok_or_else(not_a_split)?;
        let mut footer = vec![0; footer_len as usize];
        read(data_len, &mut footer)?;
        let footer: Footer = serde_json::from_slice(&footer).map_err(|_| not_a_split())?;
        let mut files = HashMap::new();
        for (name, [offset, len]) in footer.files {
            anyhow::ensure!(
                offset.checked_add(len).is_some_and(|end| end <= data_len),
                "tantivy split: {name} lies outside the file"
            );
            files.insert(PathBuf::from(name), offset..offset + len);
        }
        let index = Index::open(Bundle {
            read,
            files: Arc::new(files),
        })?;
        let payload = index.load_metas()?.payload.unwrap_or_default();
        register(&index, &options(&payload, "index")?)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        Ok(Split { index, reader })
    }

    /// Search with a tantivy query: (score, stored fields as a JSON object)
    /// per match, best first.
    pub fn search(&self, query: &str, options_json: &str) -> Result<Vec<(Score, String)>> {
        run(self, query, &options(options_json, "search")?)
    }
}

/// A bundle's files, as a read-only tantivy `Directory`.
#[derive(Clone)]
struct Bundle {
    read: Arc<ReadAt>,
    files: Arc<HashMap<PathBuf, Range<u64>>>,
}

impl std::fmt::Debug for Bundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tantivy split of {} files", self.files.len())
    }
}

fn read_only() -> io::Error {
    io::Error::other("a tantivy split is read-only")
}

impl Bundle {
    fn range(&self, p: &Path) -> Result<Range<u64>, OpenReadError> {
        self.files
            .get(p)
            .cloned()
            .ok_or_else(|| OpenReadError::FileDoesNotExist(p.to_path_buf()))
    }
}

impl tantivy::Directory for Bundle {
    fn get_file_handle(&self, p: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        Ok(Arc::new(Slice {
            read: self.read.clone(),
            range: self.range(p)?,
        }))
    }

    fn delete(&self, p: &Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(read_only()),
            filepath: p.to_path_buf(),
        })
    }

    fn exists(&self, p: &Path) -> Result<bool, OpenReadError> {
        Ok(self.files.contains_key(p))
    }

    fn open_write(&self, p: &Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::wrap_io_error(read_only(), p.to_path_buf()))
    }

    fn atomic_read(&self, p: &Path) -> Result<Vec<u8>, OpenReadError> {
        let range = self.range(p)?;
        let mut buf = vec![0; (range.end - range.start) as usize];
        (self.read)(range.start, &mut buf)
            .map_err(|e| OpenReadError::wrap_io_error(e, p.to_path_buf()))?;
        Ok(buf)
    }

    fn atomic_write(&self, _: &Path, _: &[u8]) -> io::Result<()> {
        Err(read_only())
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    /// A split never changes: nothing to lock or watch.
    fn acquire_lock(&self, _: &Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(())))
    }

    fn watch(&self, _: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

/// One file of a bundle.
struct Slice {
    read: Arc<ReadAt>,
    range: Range<u64>,
}

impl std::fmt::Debug for Slice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tantivy split bytes {:?}", self.range)
    }
}

impl HasLen for Slice {
    fn len(&self) -> usize {
        (self.range.end - self.range.start) as usize
    }
}

impl FileHandle for Slice {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let mut buf = vec![0; range.len()];
        (self.read)(self.range.start + range.start as u64, &mut buf)?;
        Ok(OwnedBytes::new(buf))
    }
}

fn run(s: &Split, query: &str, options: &SearchOptions) -> Result<Vec<(Score, String)>> {
    let schema = s.index.schema();
    let fields = match &options.fields {
        Some(names) => names
            .iter()
            .map(|n| schema.get_field(n))
            .collect::<Result<_, _>>()?,
        None => schema
            .fields()
            .filter(|(_, e)| {
                e.is_indexed()
                    && matches!(e.field_type(), FieldType::Str(_) | FieldType::JsonObject(_))
            })
            .map(|(f, _)| f)
            .collect(),
    };
    let mut parser = QueryParser::for_index(&s.index, fields);
    if options.conjunctive {
        parser.set_conjunction_by_default();
    }
    let query = if options.strict {
        parser.parse_query(query)?
    } else {
        parser.parse_query_lenient(query).0
    };
    let searcher = s.reader.searcher();
    let hits = match options.top_k {
        Some(0) => Vec::new(),
        Some(n) => searcher.search(&query, &TopDocs::with_limit(n).order_by_score())?,
        None => searcher.search(&query, &AllHits)?,
    };
    hits.into_iter()
        .map(|(score, addr)| Ok((score, stored_json(&searcher.doc(addr)?, &schema)?)))
        .collect()
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
        .collect::<Result<serde_json::Map<_, _>, _>>()?;
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

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = r#"[
        {"name": "id", "type": "i64", "options": {"stored": true, "indexed": true}},
        {"name": "body", "type": "text", "options": {"stored": false,
            "indexing": {"record": "position", "fieldnorms": true, "tokenizer": "fts"}}},
        {"name": "tag", "type": "text", "options": {"stored": true,
            "indexing": {"record": "basic", "tokenizer": "raw"}}}
    ]"#;

    const OPTIONS: &str = r#"{"tokenizers": {"fts": {"tokenizer": "simple", "filters":
        ["lowercase", "ascii_folding", {"stop_words": "english"}, {"stemmer": "porter"}]}},
        "memory_budget": 15000000}"#;

    /// Build a split in memory and open it.
    fn split(docs: &[&str]) -> (Split, Vec<u8>) {
        let build = Build::new(SCHEMA, OPTIONS).unwrap();
        for doc in docs {
            build.add(doc).unwrap();
        }
        let mut bytes = Vec::new();
        let n = build
            .finish(|b| {
                bytes.extend_from_slice(b);
                Ok(())
            })
            .unwrap();
        assert_eq!(n, docs.len() as u64);
        let shared = Arc::new(bytes.clone());
        let read = move |at: u64, buf: &mut [u8]| {
            let at = at as usize;
            buf.copy_from_slice(&shared[at..at + buf.len()]);
            Ok(())
        };
        (
            Split::open(bytes.len() as u64, Arc::new(read)).unwrap(),
            bytes,
        )
    }

    fn search(s: &Split, query: &str, options: &str) -> Vec<(f32, String)> {
        s.search(query, options).unwrap()
    }

    #[test]
    fn builds_bundles_and_searches() {
        let (s, _) = split(&[
            r#"{"id": 1, "body": "The quick brown fox jumps", "tag": "a"}"#,
            r#"{"id": 2, "body": "Small CATS and a café", "tag": ["b", "c"], "x": null}"#,
            r#"{"id": 3, "body": "cats chasing the fox", "tag": "a", "unknown": 1}"#,
        ]);
        let docs =
            |hits: Vec<(f32, String)>| -> Vec<String> { hits.into_iter().map(|h| h.1).collect() };
        // Stemmed, lowercased and folded; stored fields flatten single values.
        assert_eq!(
            docs(search(&s, "cat", "")),
            [r#"{"id":2,"tag":["b","c"]}"#, r#"{"id":3,"tag":"a"}"#]
        );
        assert_eq!(
            docs(search(&s, "cafe", "")),
            [r#"{"id":2,"tag":["b","c"]}"#]
        );
        assert!(search(&s, "the", "").is_empty());
        // Disjunctive by default, best first; conjunctive and top_k.
        assert_eq!(search(&s, "fox cats", "").len(), 3);
        assert_eq!(
            docs(search(&s, "fox cats", r#"{"top_k": 1}"#)),
            [r#"{"id":3,"tag":"a"}"#]
        );
        assert_eq!(search(&s, "fox cats", r#"{"conjunctive": true}"#).len(), 1);
        assert!(search(&s, "fox", r#"{"top_k": 0}"#).is_empty());
        // Tantivy's query language: fields, ranges, phrases.
        assert_eq!(search(&s, "tag:a AND id:[2 TO 3]", "").len(), 1);
        assert_eq!(search(&s, "\"brown fox\"", "").len(), 1);
        assert_eq!(search(&s, "a", r#"{"fields": ["tag"]}"#).len(), 2);
        assert_eq!(search(&s, "fox", r#"{"fields": null}"#).len(), 2);
        // Lenient unless strict.
        assert_eq!(search(&s, "fox nosuchfield:x", "").len(), 2);
        assert!(s.search("nosuchfield:x", r#"{"strict": true}"#).is_err());
        let scores: Vec<f32> = search(&s, "fox cats", "")
            .into_iter()
            .map(|h| h.0)
            .collect();
        assert!(scores.windows(2).all(|w| w[0] >= w[1]), "{scores:?}");
    }

    #[test]
    fn merges_into_one_segment() {
        let docs: Vec<String> = (0..20_000)
            .map(|i| {
                format!(
                    r#"{{"id": {i}, "body": "doc {i} {}"}}"#,
                    if i % 10 == 0 { "fox" } else { "cat" }
                )
            })
            .collect();
        let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
        let (s, bytes) = split(&docs);
        assert_eq!(s.reader.searcher().segment_readers().len(), 1);
        assert_eq!(search(&s, "fox", "").len(), 2_000);
        assert_eq!(search(&s, "fox", r#"{"top_k": 5}"#).len(), 5);
        assert_eq!(&bytes[bytes.len() - 8..], MAGIC);
    }

    #[test]
    fn rejects_bad_input() {
        assert!(Build::new("not json", "").is_err());
        assert!(Build::new(SCHEMA, r#"{"nope": 1}"#).is_err());
        let b = Build::new(SCHEMA, OPTIONS).unwrap();
        assert!(b.add("[1]").is_err());
        assert!(b.add(r#"{"id": "x"}"#).is_err());
        assert!(options::<SearchOptions>(r#"{"limit": 1}"#, "search").is_err());
        let bad = options::<BuildOptions>(
            r#"{"tokenizers": {"x": {"filters": [{"stemmer": "klingon"}]}}}"#,
            "index",
        )
        .unwrap();
        assert!(register(&Index::create_in_ram(Schema::builder().build()), &bad).is_err());
        assert_eq!(language("English").unwrap(), Language::English);
        assert_eq!(language("porter").unwrap(), Language::English);
        assert!(language("").is_err());
        let read = |_: u64, buf: &mut [u8]| {
            buf.fill(0);
            Ok(())
        };
        assert!(Split::open(100, Arc::new(read)).is_err());
        assert!(Split::open(3, Arc::new(read)).is_err());
    }
}
