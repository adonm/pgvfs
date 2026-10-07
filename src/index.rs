//! Tantivy indexes stored as pgvfs files, for full-text search from DuckDB.
//!
//! The index at pgvfs://<volume>/<path> is `<path>/current`, naming the live
//! generation, plus each generation's tantivy files under `<path>/gen-<id>/`.
//! A build writes a whole new generation with one tantivy `IndexWriter` (so
//! only in the pgvfs writer), commits it, publishes `current`, and removes the
//! generations it replaced. A published generation never changes, so readers
//! take no locks: they open the generation `current` names (cached per
//! connection), and a replaced one stays readable for the reap grace period.
//!
//! Tantivy's own formats pass straight through as JSON: the schema, documents,
//! index settings and the query language. Build options may also name
//! tokenizers (`Analyzer`); they ride in the commit payload so every reader
//! registers the same ones.

use std::collections::{BTreeMap, HashMap};
use std::io::{self, BufWriter, Read, Seek, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context as _, Result};
use serde::Deserialize;
use tantivy::collector::{Collector, SegmentCollector, TopDocs};
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AntiCallToken, DirectoryLock, FileHandle, Lock, OwnedBytes, TerminatingWrite, WatchCallback,
    WatchHandle, WritePtr,
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

use crate::store::FileInfo;
use crate::{PgvfsConn, PgvfsWriter};

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

/// Search options, as JSON: `limit` (default: every hit), the default
/// `fields` for terms that name none (default: every indexed text field),
/// `conjunctive` (all terms must match) and `strict` (fail on query syntax
/// errors instead of dropping what tantivy cannot parse).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchOptions {
    limit: Option<usize>,
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

fn index_path(path: &str) -> Result<String> {
    let path = path.trim_matches('/');
    anyhow::ensure!(
        !path.is_empty(),
        "a tantivy index needs a directory: pgvfs://<volume>/<path>"
    );
    Ok(path.to_owned())
}

/// One generation's files, as a tantivy `Directory`.
#[derive(Clone)]
struct Dir {
    conn: &'static PgvfsConn,
    volume: Arc<str>,
    /// `<path>/<generation>/`
    prefix: Arc<str>,
    /// Set when a build is abandoned: tantivy threads still finishing (the
    /// doc store's compressor) must not publish files after its cleanup.
    abandoned: Arc<AtomicBool>,
}

impl std::fmt::Debug for Dir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pgvfs://{}/{}", self.volume, self.prefix)
    }
}

fn io_err(e: anyhow::Error) -> io::Error {
    io::Error::other(format!("{e:#}"))
}

impl Dir {
    fn path(&self, p: &Path) -> io::Result<String> {
        let p = p
            .to_str()
            .ok_or_else(|| io::Error::other("tantivy paths must be UTF-8"))?;
        Ok(format!("{}{p}", self.prefix))
    }

    /// Publish a whole file, one upload at a time (see `FileWrite`).
    fn upload(&self, path: &str, mut data: impl Read) -> io::Result<()> {
        let _one = UPLOAD.lock().unwrap_or_else(|e| e.into_inner());
        if self.abandoned.load(Ordering::Relaxed) {
            return Err(io::Error::other("index build abandoned"));
        }
        let mut writer = PgvfsWriter::open(self.conn, &self.volume, path).map_err(io_err)?;
        let mut buf = vec![0; 1 << 16];
        loop {
            let n = data.read(&mut buf)?;
            if n == 0 {
                return writer.publish().map_err(io_err);
            }
            writer.write(&buf[..n]).map_err(io_err)?;
        }
    }

    fn open(&self, p: &Path) -> Result<FileInfo, OpenReadError> {
        let wrap = |e| OpenReadError::wrap_io_error(e, p.to_path_buf());
        match self.conn.open(&self.volume, &self.path(p).map_err(wrap)?) {
            Ok(Some(f)) => Ok(f),
            Ok(None) => Err(OpenReadError::FileDoesNotExist(p.to_path_buf())),
            Err(e) => Err(wrap(io_err(e))),
        }
    }
}

impl tantivy::Directory for Dir {
    fn get_file_handle(&self, p: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        Ok(Arc::new(Handle {
            conn: self.conn,
            file: self.open(p)?,
        }))
    }

    fn delete(&self, p: &Path) -> Result<(), DeleteError> {
        let wrap = |e| DeleteError::IoError {
            io_error: Arc::new(e),
            filepath: p.to_path_buf(),
        };
        match self.conn.remove(&self.volume, &self.path(p).map_err(wrap)?) {
            Ok(true) => Ok(()),
            Ok(false) => Err(DeleteError::FileDoesNotExist(p.to_path_buf())),
            Err(e) => Err(wrap(io_err(e))),
        }
    }

    fn exists(&self, p: &Path) -> Result<bool, OpenReadError> {
        match self.open(p) {
            Ok(_) => Ok(true),
            Err(OpenReadError::FileDoesNotExist(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn open_write(&self, p: &Path) -> Result<WritePtr, OpenWriteError> {
        let wrap = |e| OpenWriteError::wrap_io_error(e, p.to_path_buf());
        if self.exists(p).map_err(|e| wrap(io::Error::other(e)))? {
            return Err(OpenWriteError::FileAlreadyExists(p.to_path_buf()));
        }
        Ok(BufWriter::new(Box::new(FileWrite {
            dir: self.clone(),
            path: self.path(p).map_err(wrap)?,
            spool: Some(tempfile::tempfile().map_err(wrap)?),
        })))
    }

    fn atomic_read(&self, p: &Path) -> Result<Vec<u8>, OpenReadError> {
        let file = self.open(p)?;
        let mut buf = vec![0; file.size as usize];
        self.conn
            .read(&file, 0, &mut buf)
            .map_err(|e| OpenReadError::wrap_io_error(io_err(e), p.to_path_buf()))?;
        Ok(buf)
    }

    fn atomic_write(&self, p: &Path, data: &[u8]) -> io::Result<()> {
        self.upload(&self.path(p)?, data)
    }

    /// Publishing a file commits it.
    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    /// A generation has one `IndexWriter`, in the one pgvfs writer process,
    /// and never changes once published: nothing to lock (lock files would
    /// also need write access, which readers lack) and nothing to watch.
    fn acquire_lock(&self, _: &Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(())))
    }

    fn watch(&self, _: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

struct Handle {
    conn: &'static PgvfsConn,
    file: FileInfo,
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pgvfs file {}", self.file.file_id)
    }
}

impl HasLen for Handle {
    fn len(&self) -> usize {
        self.file.size as usize
    }
}

impl FileHandle for Handle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let mut buf = vec![0; range.len()];
        self.conn
            .read(&self.file, range.start as i64, &mut buf)
            .map_err(io_err)?;
        Ok(OwnedBytes::new(buf))
    }
}

/// Tantivy writes all of a segment's files at once, interleaved, and a pgvfs
/// writer holds a pooled connection until it publishes: so each file spools
/// to a local temporary file, uploaded whole by `terminate`, one upload at a
/// time. Dropped without `terminate`, it is discarded.
struct FileWrite {
    dir: Dir,
    path: String,
    spool: Option<std::fs::File>,
}

static UPLOAD: Mutex<()> = Mutex::new(());

fn published() -> io::Error {
    io::Error::other("pgvfs file already published")
}

impl Write for FileWrite {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.spool.as_mut().ok_or_else(published)?.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for FileWrite {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        let mut spool = self.spool.take().ok_or_else(published)?;
        spool.rewind()?;
        self.dir.upload(&self.path, spool)
    }
}

/// Index paths with a build running in this process.
static BUILDING: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

/// A generation being built: unless published, dropping it removes its
/// files. Either way it frees the index path for the next build.
struct Generation {
    conn: &'static PgvfsConn,
    volume: String,
    path: String,
    name: String,
    published: bool,
    abandoned: Arc<AtomicBool>,
}

impl Drop for Generation {
    fn drop(&mut self) {
        if !self.published {
            self.abandoned.store(true, Ordering::Relaxed);
            // Wait out an upload in flight; later ones see `abandoned`.
            let _uploads = UPLOAD.lock().unwrap_or_else(|e| e.into_inner());
            if let Err(e) =
                remove_generations(self.conn, &self.volume, &self.path, |g| g == self.name)
            {
                eprintln!("pgvfs: could not remove an abandoned index build: {e:#}");
            }
        }
        let key = (self.volume.clone(), self.path.clone());
        BUILDING.lock().unwrap().retain(|k| *k != key);
    }
}

/// A new index generation being written.
pub struct Build {
    // Dropped first: the writer's threads stop before `generation` cleans up.
    writer: Option<IndexWriter>,
    index: Index,
    schema: Schema,
    payload: String,
    merge: bool,
    docs: AtomicU64,
    generation: Generation,
}

impl Build {
    pub fn open(
        conn: &'static PgvfsConn,
        volume: &str,
        path: &str,
        schema: &str,
        options_json: &str,
    ) -> Result<Build> {
        let path = index_path(path)?;
        let schema: Schema = serde_json::from_str(schema).context("tantivy schema")?;
        let options: BuildOptions = options(options_json, "index")?;
        let key = (volume.to_owned(), path.clone());
        {
            let mut building = BUILDING.lock().unwrap();
            anyhow::ensure!(
                !building.contains(&key),
                "pgvfs://{volume}/{path} is already being built"
            );
            building.push(key);
        }
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let generation = Generation {
            conn,
            volume: volume.into(),
            path: path.clone(),
            name: format!("gen-{nanos:x}"),
            published: false,
            abandoned: Arc::default(),
        };
        let dir = Dir {
            conn,
            volume: volume.into(),
            prefix: format!("{path}/{}/", generation.name).into(),
            abandoned: generation.abandoned.clone(),
        };
        let index = Index::create(dir, schema.clone(), options.settings.clone())?;
        register(&index, &options)?;
        let writer: IndexWriter = index.writer(options.memory_budget)?;
        if options.merge {
            writer.set_merge_policy(Box::new(NoMergePolicy));
        }
        Ok(Build {
            writer: Some(writer),
            index,
            schema,
            payload: if options_json.trim().is_empty() {
                "{}".into()
            } else {
                options_json.into()
            },
            merge: options.merge,
            docs: AtomicU64::new(0),
            generation,
        })
    }

    /// Add a document: a JSON object of field values (a value or an array of
    /// them), parsed as tantivy does; nulls and unknown fields are skipped.
    pub fn add(&self, json: &str) -> Result<()> {
        let mut doc: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(json).context("a tantivy document is a JSON object")?;
        doc.retain(|_, v| !v.is_null());
        let doc = TantivyDocument::from_json_object(&self.schema, doc)?;
        self.writer
            .as_ref()
            .context("index build already committed")?
            .add_document(doc)?;
        self.docs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Commit, publish as the index at this path, and remove the generations
    /// it replaces. Returns the documents indexed.
    pub fn commit(mut self) -> Result<u64> {
        let mut writer = self
            .writer
            .take()
            .context("index build already committed")?;
        let mut commit = writer.prepare_commit()?;
        commit.set_payload(&self.payload);
        commit.commit()?;
        if self.merge {
            let segments = self.index.searchable_segment_ids()?;
            if segments.len() > 1 {
                writer.merge(&segments).wait()?;
            }
        }
        writer.wait_merging_threads()?;

        let g = &mut self.generation;
        let mut current = PgvfsWriter::open(g.conn, &g.volume, &format!("{}/current", g.path))?;
        current.write(g.name.as_bytes())?;
        current.publish()?;
        g.published = true;
        if let Err(e) = remove_generations(g.conn, &g.volume, &g.path, |name| name != g.name) {
            eprintln!("pgvfs: could not remove replaced index generations: {e:#}");
        }
        Ok(self.docs.load(Ordering::Relaxed))
    }
}

/// Remove the files of generations under `path` that `doomed` picks.
fn remove_generations(
    conn: &PgvfsConn,
    volume: &str,
    path: &str,
    doomed: impl Fn(&str) -> bool,
) -> Result<()> {
    let prefix = format!("{path}/");
    let mut files = Vec::new();
    conn.list(volume, &prefix, -1, |p| {
        if let Some((generation, _)) = p[prefix.len()..].split_once('/') {
            if generation.starts_with("gen-") && doomed(generation) {
                files.push(p.to_owned());
            }
        }
    })?;
    for file in files {
        conn.remove(volume, &file)?;
    }
    Ok(())
}

/// Remove the index at `path`. Returns false if there was none.
pub fn drop_index(conn: &PgvfsConn, volume: &str, path: &str) -> Result<bool> {
    let path = index_path(path)?;
    let found = conn.remove(volume, &format!("{path}/current"))?;
    remove_generations(conn, volume, &path, |_| true)?;
    conn.indexes
        .0
        .lock()
        .unwrap()
        .remove(&(volume.to_owned(), path));
    Ok(found)
}

/// Open indexes by (volume, path).
#[derive(Default)]
pub struct Cache(Mutex<HashMap<(String, String), Arc<Searchable>>>);

struct Searchable {
    /// The file_id of the `current` that named this generation.
    current: i64,
    index: Index,
    reader: IndexReader,
}

fn open_index(conn: &'static PgvfsConn, volume: &str, path: &str) -> Result<Arc<Searchable>> {
    let current = conn
        .open(volume, &format!("{path}/current"))?
        .ok_or_else(|| anyhow!("no tantivy index at pgvfs://{volume}/{path}"))?;
    let key = (volume.to_owned(), path.to_owned());
    if let Some(s) = conn.indexes.0.lock().unwrap().get(&key) {
        if s.current == current.file_id {
            return Ok(s.clone());
        }
    }
    let mut name = vec![0; current.size as usize];
    conn.read(&current, 0, &mut name)?;
    let generation = String::from_utf8(name)?;
    let index = Index::open(Dir {
        conn,
        volume: volume.into(),
        prefix: format!("{path}/{generation}/").into(),
        abandoned: Arc::default(),
    })?;
    let payload = index.load_metas()?.payload.unwrap_or_default();
    register(&index, &options(&payload, "index")?)?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searchable = Arc::new(Searchable {
        current: current.file_id,
        index,
        reader,
    });
    conn.indexes
        .0
        .lock()
        .unwrap()
        .insert(key, searchable.clone());
    Ok(searchable)
}

/// Search the index at `path` with a tantivy query, calling `hit` with each
/// match's score and stored fields as a JSON object, best first.
pub fn search(
    conn: &'static PgvfsConn,
    volume: &str,
    path: &str,
    query: &str,
    options_json: &str,
    mut hit: impl FnMut(Score, &str),
) -> Result<()> {
    let path = index_path(path)?;
    let options: SearchOptions = options(options_json, "search")?;
    let attempt = || run(&*open_index(conn, volume, &path)?, query, &options);
    let hits = attempt().or_else(|_| {
        // `current` is cached for up to PGVFS_OPEN_CACHE_S: it may name a
        // generation that has since been replaced and removed.
        conn.files.forget(volume, &format!("{path}/current"));
        attempt()
    })?;
    for (score, doc) in &hits {
        hit(*score, doc);
    }
    Ok(())
}

fn run(s: &Searchable, query: &str, options: &SearchOptions) -> Result<Vec<(Score, String)>> {
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
    let hits = match options.limit {
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

    fn ram_index(schema: &str, options: &str, docs: &[&str]) -> Searchable {
        let schema: Schema = serde_json::from_str(schema).unwrap();
        let index = Index::create_in_ram(schema.clone());
        register(&index, &super::options(options, "index").unwrap()).unwrap();
        let mut writer: IndexWriter = index.writer(15 << 20).unwrap();
        for doc in docs {
            writer
                .add_document(TantivyDocument::parse_json(&schema, doc).unwrap())
                .unwrap();
        }
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        Searchable {
            current: 0,
            index,
            reader,
        }
    }

    const SCHEMA: &str = r#"[
        {"name": "id", "type": "i64", "options": {"stored": true, "indexed": true}},
        {"name": "body", "type": "text", "options": {"stored": false,
            "indexing": {"record": "position", "fieldnorms": true, "tokenizer": "fts"}}},
        {"name": "tag", "type": "text", "options": {"stored": true,
            "indexing": {"record": "basic", "tokenizer": "raw"}}}
    ]"#;

    const OPTIONS: &str = r#"{"tokenizers": {"fts": {"tokenizer": "simple", "filters":
        ["lowercase", "ascii_folding", {"stop_words": "english"}, {"stemmer": "porter"}]}}}"#;

    fn search(s: &Searchable, query: &str, options: &str) -> Vec<(f32, String)> {
        run(s, query, &super::options(options, "search").unwrap()).unwrap()
    }

    #[test]
    fn searches_with_custom_tokenizers_and_options() {
        let s = ram_index(
            SCHEMA,
            OPTIONS,
            &[
                r#"{"id": 1, "body": "The quick brown fox jumps", "tag": "a"}"#,
                r#"{"id": 2, "body": "Small CATS and a café", "tag": ["b", "c"]}"#,
                r#"{"id": 3, "body": "cats chasing the fox", "tag": "a"}"#,
            ],
        );
        let ids =
            |hits: Vec<(f32, String)>| -> Vec<String> { hits.into_iter().map(|h| h.1).collect() };
        // Stemmed, lowercased and folded; stored fields flatten single values.
        assert_eq!(
            ids(search(&s, "cat", "")),
            [r#"{"id":2,"tag":["b","c"]}"#, r#"{"id":3,"tag":"a"}"#]
        );
        assert_eq!(ids(search(&s, "cafe", "")), [r#"{"id":2,"tag":["b","c"]}"#]);
        // Disjunctive by default, best first; conjunctive and limit.
        assert_eq!(search(&s, "fox cats", "").len(), 3);
        assert_eq!(
            ids(search(&s, "fox cats", r#"{"limit": 1}"#)),
            [r#"{"id":3,"tag":"a"}"#]
        );
        assert_eq!(search(&s, "fox cats", r#"{"conjunctive": true}"#).len(), 1);
        assert!(search(&s, "fox", r#"{"limit": 0}"#).is_empty());
        // Tantivy's query language: fields, ranges, phrases.
        assert_eq!(search(&s, "tag:a AND id:[2 TO 3]", "").len(), 1);
        assert_eq!(search(&s, "\"brown fox\"", "").len(), 1);
        assert_eq!(search(&s, "a", r#"{"fields": ["tag"]}"#).len(), 2);
        // Lenient unless strict.
        assert_eq!(search(&s, "fox nosuchfield:x", "").len(), 2);
        assert!(run(
            &s,
            "nosuchfield:x",
            &options(r#"{"strict": true}"#, "search").unwrap()
        )
        .is_err());
        let scores: Vec<f32> = search(&s, "fox cats", "")
            .into_iter()
            .map(|h| h.0)
            .collect();
        assert!(scores.windows(2).all(|w| w[0] >= w[1]), "{scores:?}");
    }

    #[test]
    fn rejects_bad_options() {
        assert!(options::<SearchOptions>(r#"{"limt": 1}"#, "search").is_err());
        assert!(options::<BuildOptions>(
            r#"{"tokenizers": {"x": {"filters": ["nope"]}}}"#,
            "index"
        )
        .is_err());
        let bad = options::<BuildOptions>(
            r#"{"tokenizers": {"x": {"filters": [{"stemmer": "klingon"}]}}}"#,
            "index",
        )
        .unwrap();
        assert!(register(&Index::create_in_ram(Schema::builder().build()), &bad).is_err());
        assert_eq!(language("English").unwrap(), Language::English);
        assert_eq!(language("porter").unwrap(), Language::English);
        assert!(language("").is_err());
    }
}
