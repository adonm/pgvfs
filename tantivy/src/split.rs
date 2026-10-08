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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Read, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context as _, Result};
use serde::{Deserialize, Serialize};
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    DirectoryLock, FileHandle, Lock, OwnedBytes, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::indexer::IndexWriterOptions;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::schema::Schema;
use tantivy::tokenizer::{
    AlphaNumOnlyFilter, AsciiFoldingFilter, Language, LowerCaser, NgramTokenizer, RawTokenizer,
    RegexTokenizer, RemoveLongFilter, SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer,
    WhitespaceTokenizer,
};
use tantivy::{
    HasLen, Index, IndexReader, IndexSettings, IndexWriter, ReloadPolicy, TantivyDocument,
};

use crate::search::{check_exclusion_key, AliveCache, Exclude, InSetQuery};

const MAGIC: &[u8; 8] = b"tantivy1";

/// Build options, as JSON: tokenizers to register by name, tantivy
/// `IndexSettings`, the writer's memory budget in bytes, and whether to merge
/// the new index into one segment (one read per term per query).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BuildOptions {
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

pub(crate) fn options<'a, T: Deserialize<'a>>(json: &'a str, what: &str) -> Result<T> {
    let json = if json.trim().is_empty() { "{}" } else { json };
    serde_json::from_str(json).with_context(|| format!("tantivy {what} options"))
}

/// A tokenizer and its filters, built into a tantivy `TextAnalyzer`:
/// `{"tokenizer": "simple", "filters": ["lowercase", {"stemmer": "english"}]}`.
#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Analyzer {
    #[serde(default)]
    tokenizer: TokenizerSpec,
    #[serde(default)]
    filters: Vec<FilterSpec>,
}

#[derive(Deserialize, Default, PartialEq)]
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

#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum FilterSpec {
    Lowercase,
    AsciiFolding,
    AlphaNumOnly,
    RemoveLong(usize),
    StopWords(StopWords),
    Stemmer(String),
}

#[derive(Deserialize, PartialEq)]
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
pub(crate) fn register(index: &Index, options: &BuildOptions) -> Result<()> {
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

/// Memory a build holds beyond its `memory_budget`: segment and document-store buffers.
const BUILD_OVERHEAD: u64 = 10 << 20;
/// tantivy's smallest arena per indexing thread (its MEMORY_BUDGET_NUM_BYTES_MIN).
const MIN_ARENA_PER_THREAD: usize = 15_000_000;

impl Build {
    /// Opens a build: `threads` indexing threads share its `memory_budget`, with one merge
    /// thread and no automatic merging. `live` builds of the same query are already open, and
    /// `max_memory` is DuckDB's memory limit (0: none). tantivy's memory is not DuckDB's, so a
    /// build that would take the total past the limit is refused.
    pub fn new(
        schema: &str,
        options_json: &str,
        threads: usize,
        live: usize,
        max_memory: u64,
    ) -> Result<Build> {
        let schema: Schema = serde_json::from_str(schema).context("tantivy schema")?;
        let options: BuildOptions = options(options_json, "index")?;
        let needed = (live as u64 + 1) * (options.memory_budget as u64 + BUILD_OVERHEAD);
        anyhow::ensure!(
            max_memory == 0 || needed <= max_memory,
            "tantivy_index: {} open builds need about {needed} bytes, over memory_limit \
             ({max_memory} bytes): lower memory_budget, build fewer groups per query, or raise \
             memory_limit",
            live + 1
        );
        let dir = tempfile::Builder::new()
            .prefix("duckdb-tantivy-")
            .tempdir()?;
        let index = Index::builder()
            .schema(schema.clone())
            .settings(options.settings.clone())
            .create_in_dir(dir.path())?;
        register(&index, &options)?;
        let threads = threads.clamp(1, (options.memory_budget / MIN_ARENA_PER_THREAD).max(1));
        let writer: IndexWriter = index.writer_with_options(
            IndexWriterOptions::builder()
                .num_worker_threads(threads)
                .memory_budget_per_thread(options.memory_budget / threads)
                .num_merge_threads(1)
                .build(),
        )?;
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
    pub fn finish(self, out: impl FnMut(&[u8]) -> Result<()>) -> Result<u64> {
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
        bundle(&index, dir.path(), out)?;
        Ok(docs.load(Ordering::Relaxed))
    }
}

/// Write a local index's live files (meta.json and its segments') as a
/// bundle to `out`.
fn bundle(index: &Index, dir: &Path, mut out: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
    let mut names: Vec<PathBuf> = vec![PathBuf::from("meta.json")];
    for segment in index.searchable_segment_metas()? {
        // list_files names a delete file whether or not there is one.
        names.extend(
            segment
                .list_files()
                .into_iter()
                .filter(|f| dir.join(f).exists()),
        );
    }
    names.sort();
    names.dedup();
    let mut footer = Footer::default();
    let mut offset = 0u64;
    let mut buf = vec![0; 1 << 20];
    for name in names {
        let mut file = std::fs::File::open(dir.join(&name))
            .with_context(|| format!("tantivy file {}", name.display()))?;
        let start = offset;
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            out(&buf[..n])?;
            offset += n as u64;
        }
        let name = name
            .into_os_string()
            .into_string()
            .map_err(|_| anyhow!("non-UTF-8 file name"))?;
        footer.files.insert(name, [start, offset - start]);
    }
    let footer = serde_json::to_vec(&footer)?;
    out(&footer)?;
    out(&(footer.len() as u64).to_le_bytes())?;
    out(MAGIC)?;
    Ok(())
}

/// Merge options, as JSON: the fast field `exclude` holds values of, and the
/// writer's memory budget.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MergeOptions {
    exclude_field: Option<String>,
    #[serde(default = "default_memory_budget")]
    memory_budget: usize,
}

/// Merge splits of one schema into a single split without the documents
/// `exclude` names, written to `out`. Returns the documents kept. The
/// sources' segments are copied as they are, not re-indexed.
pub fn merge(
    splits: &[&Split],
    options_json: &str,
    exclude: Option<&Exclude>,
    out: impl FnMut(&[u8]) -> Result<()>,
) -> Result<u64> {
    let options: MergeOptions = options(options_json, "merge")?;
    let first = splits
        .first()
        .context("tantivy_merge needs at least one split")?;
    let schema = first.index.schema();
    anyhow::ensure!(
        splits.iter().all(|s| s.index.schema() == schema),
        "tantivy_merge: the splits have different schemas"
    );
    let payload = first.index.load_metas()?.payload.unwrap_or_default();
    let build_options: BuildOptions = self::options(&payload, "index")?;
    let exclude_field = match exclude {
        Some(_) => Some(
            options
                .exclude_field
                .as_deref()
                .context("tantivy_merge: exclude needs options.exclude_field")?,
        ),
        None => None,
    };
    for split in splits {
        let payload = split.index.load_metas()?.payload.unwrap_or_default();
        let source_options: BuildOptions = self::options(&payload, "index")?;
        anyhow::ensure!(
            source_options.tokenizers == build_options.tokenizers,
            "tantivy_merge: the splits have different tokenizers"
        );
        if let Some(field) = exclude_field {
            check_exclusion_key(split, field)?;
        }
    }
    let dir = tempfile::Builder::new()
        .prefix("duckdb-tantivy-")
        .tempdir()?;
    let index = Index::builder()
        .schema(schema)
        .settings(first.index.settings().clone())
        .create_in_dir(dir.path())?;
    register(&index, &build_options)?;
    let mut writer: IndexWriter = index.writer(options.memory_budget)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    let mut seen = HashSet::new();
    for split in splits {
        for meta in split.index.searchable_segment_metas()? {
            anyhow::ensure!(
                !meta.has_deletes(),
                "tantivy_merge: a split has deleted documents"
            );
            anyhow::ensure!(
                seen.insert(meta.id()),
                "tantivy_merge: a segment appears twice"
            );
            for file in meta.list_files() {
                if split.bundle.files.contains_key(&file) {
                    split.bundle.copy(&file, &dir.path().join(&file))?;
                }
            }
            writer.add_segment(index.new_segment_meta(meta.id(), meta.max_doc()))?;
        }
    }
    if let (Some(exclude), Some(field)) = (exclude, exclude_field) {
        writer.delete_query(Box::new(InSetQuery::new(field.to_owned(), exclude)))?;
    }
    let mut commit = writer.prepare_commit()?;
    commit.set_payload(&payload);
    commit.commit()?;
    let segments = index.searchable_segment_ids()?;
    if !segments.is_empty() {
        writer.merge(&segments).wait()?;
    }
    writer.wait_merging_threads()?;
    bundle(&index, dir.path(), out)?;
    let docs = index
        .searchable_segment_metas()?
        .iter()
        .map(|m| m.num_docs() as u64)
        .sum();
    Ok(docs)
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
    pub(crate) index: Index,
    pub(crate) reader: IndexReader,
    pub(crate) bundle: Bundle,
    pub(crate) alive: AliveCache,
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
        let bundle = Bundle {
            read,
            files: Arc::new(files),
        };
        let index = Index::open(bundle.clone())?;
        let payload = index.load_metas()?.payload.unwrap_or_default();
        register(&index, &options(&payload, "index")?)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        Ok(Split {
            index,
            reader,
            bundle,
            alive: AliveCache::default(),
        })
    }
}

/// A bundle's files, as a read-only tantivy `Directory`.
#[derive(Clone)]
pub(crate) struct Bundle {
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

    /// Copy one of the bundle's files to a local path.
    fn copy(&self, p: &Path, to: &Path) -> Result<()> {
        let range = self.range(p)?;
        let mut file = std::fs::File::create(to)?;
        let mut buf = vec![0; 1 << 20];
        let mut at = range.start;
        while at < range.end {
            let n = (range.end - at).min(buf.len() as u64) as usize;
            (self.read)(at, &mut buf[..n])?;
            file.write_all(&buf[..n])?;
            at += n as u64;
        }
        Ok(())
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

#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    pub const SCHEMA: &str = r#"[
        {"name": "id", "type": "i64", "options": {"stored": true, "indexed": true, "fast": true}},
        {"name": "body", "type": "text", "options": {"stored": false,
            "indexing": {"record": "position", "fieldnorms": true, "tokenizer": "fts"}}},
        {"name": "tag", "type": "text", "options": {"stored": true, "fast": true,
            "indexing": {"record": "basic", "tokenizer": "raw"}}}
    ]"#;

    pub const OPTIONS: &str = r#"{"tokenizers": {"fts": {"tokenizer": "simple", "filters":
        ["lowercase", "ascii_folding", {"stop_words": "english"}, {"stemmer": "porter"}]}},
        "memory_budget": 15000000}"#;

    /// A split's bytes.
    pub fn build(docs: &[&str]) -> Vec<u8> {
        build_with(SCHEMA, OPTIONS, docs)
    }

    pub fn build_with(schema: &str, options: &str, docs: &[&str]) -> Vec<u8> {
        let build = Build::new(schema, options, 1, 0, 0).unwrap();
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
        bytes
    }

    pub fn open(bytes: Vec<u8>) -> Split {
        let size = bytes.len() as u64;
        let read = move |at: u64, buf: &mut [u8]| {
            let at = at as usize;
            buf.copy_from_slice(&bytes[at..at + buf.len()]);
            Ok(())
        };
        Split::open(size, Arc::new(read)).unwrap()
    }

    pub fn split(docs: &[&str]) -> Split {
        open(build(docs))
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;
    use crate::search::Request;

    fn ids(splits: &[&Split], query: &str) -> Vec<i64> {
        let mut ids: Vec<i64> = Request::new(splits, query, r#"{"fast": ["id"]}"#, None)
            .unwrap()
            .hits()
            .unwrap()
            .into_iter()
            .map(|h| {
                serde_json::from_str::<serde_json::Value>(&h.doc).unwrap()["id"]
                    .as_i64()
                    .unwrap()
            })
            .collect();
        ids.sort();
        ids
    }

    #[test]
    fn bundles_one_segment_of_live_files() {
        let docs: Vec<String> = (0..20_000)
            .map(|i| {
                format!(
                    r#"{{"id": {i}, "body": "doc {i} {}"}}"#,
                    if i % 10 == 0 { "fox" } else { "cat" }
                )
            })
            .collect();
        let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
        let bytes = build(&docs);
        assert_eq!(&bytes[bytes.len() - 8..], MAGIC);
        let s = open(bytes);
        assert_eq!(s.reader.searcher().segment_readers().len(), 1);
        // meta.json and one segment's files, no leftovers of merged segments
        let segment = s.index.searchable_segment_metas().unwrap()[0].list_files();
        assert!(s
            .bundle
            .files
            .keys()
            .all(|f| segment.contains(f) || f.as_os_str() == "meta.json"));
        assert_eq!(ids(&[&s], "fox").len(), 2_000);
    }

    #[test]
    fn merges_splits_without_excluded_documents() {
        let a = split(&[r#"{"id": 1, "body": "fox"}"#, r#"{"id": 2, "body": "cat"}"#]);
        let b = split(&[
            r#"{"id": 3, "body": "fox"}"#,
            r#"{"id": 4, "body": "fox and cat"}"#,
        ]);
        let mut bytes = Vec::new();
        let kept = merge(
            &[&a, &b],
            r#"{"exclude_field": "id"}"#,
            Some(&Exclude::from_ids([3])),
            |b| {
                bytes.extend_from_slice(b);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(kept, 3);
        let m = open(bytes);
        assert_eq!(m.reader.searcher().segment_readers().len(), 1);
        assert_eq!(ids(&[&m], "fox"), [1, 4]);
        assert_eq!(ids(&[&m], "cat"), [2, 4]); // stemming kept: tokenizers ride along
        assert!(merge(&[], "", None, |_| Ok(())).is_err());
        assert!(merge(&[&a], "", Some(&Exclude::from_ids([1])), |_| Ok(())).is_err());
        assert!(merge(&[&a, &a], "", None, |_| Ok(())).is_err());
        assert!(merge(
            &[&a],
            r#"{"exclude_field":"missing"}"#,
            Some(&Exclude::from_ids([1])),
            |_| Ok(())
        )
        .is_err());
        let different = open(build_with(
            SCHEMA,
            r#"{"tokenizers":{"fts":{"tokenizer":"raw"}},"memory_budget":15000000}"#,
            &[r#"{"id":5,"body":"fox"}"#],
        ));
        assert!(merge(&[&a, &different], "", None, |_| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("different tokenizers"));
        let other_schema = open(build_with("[]", "", &["{}"]));
        assert!(merge(&[&a, &other_schema], "", None, |_| Ok(())).is_err());
        assert!(Request::new(&[&a, &other_schema], "*", r#"{"global_stats":true}"#, None).is_err());
    }

    #[test]
    fn merges_empty_splits_and_all_excluded_documents() {
        let a = split(&[r#"{"id":1,"body":"fox"}"#]);
        let mut bytes = Vec::new();
        assert_eq!(
            merge(
                &[&a],
                r#"{"exclude_field":"id"}"#,
                Some(&Exclude::from_ids([1])),
                |b| {
                    bytes.extend_from_slice(b);
                    Ok(())
                }
            )
            .unwrap(),
            0
        );
        let empty = open(bytes);
        assert_eq!(
            Request::new(&[&empty], "*", "", None)
                .unwrap()
                .count()
                .unwrap(),
            0
        );
        assert_eq!(merge(&[&empty], "", None, |_| Ok(())).unwrap(), 0);
    }

    #[test]
    fn rejects_bad_input() {
        assert!(Build::new("not json", "", 1, 0, 0).is_err());
        assert!(Build::new(SCHEMA, r#"{"nope": 1}"#, 1, 0, 0).is_err());
        let b = Build::new(SCHEMA, OPTIONS, 1, 0, 0).unwrap();
        assert!(b.add("[1]").is_err());
        assert!(b.add(r#"{"id": "x"}"#).is_err());
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
