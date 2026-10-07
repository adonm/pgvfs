//! OpenSearch query DSL, a subset, compiled to tantivy queries, so callers
//! that speak Elasticsearch/OpenSearch (as Quickwit does) need no query-string
//! step. A query is `{"<type>": body}`, or a request body `{"query": ...}`.
//!
//! Supported: match_all, match_none, bool (must, should, must_not, filter,
//! minimum_should_match), constant_score, dis_max, term, terms, match
//! (operator, minimum_should_match, fuzziness), match_phrase (slop),
//! match_phrase_prefix, multi_match (best_fields, most_fields, phrase,
//! phrase_prefix), prefix, wildcard, regexp, fuzzy, exists, range,
//! more_like_this (text), query_string and simple_query_string; `boost` where Elasticsearch takes it,
//! `case_insensitive` on term, prefix, wildcard and regexp. Filters
//! (`bool.filter`, `constant_score`) do not score.

use std::ops::Bound;

use anyhow::{anyhow, bail, Context as _, Result};
use serde_json::Value;
use tantivy::query::{
    AllQuery, BooleanQuery, BoostQuery, ConstScoreQuery, DisjunctionMaxQuery, EmptyQuery,
    EnableScoring, ExistsQuery, FuzzyTermQuery, MoreLikeThisQuery, Occur, PhrasePrefixQuery,
    PhraseQuery, Query, QueryParser, RangeQuery, TermQuery, TermSetQuery, Weight,
};
use tantivy::schema::{Facet, Field, FieldType, IndexRecordOption, OwnedValue, Schema};
use tantivy::{DateTime, Index, Term};

use crate::pattern::PatternQuery;

/// `ignore_unmapped`: a field the split lacks matches nothing, as in
/// Elasticsearch, instead of failing the query.
pub fn compile(index: &Index, dsl: &Value, ignore_unmapped: bool) -> Result<Box<dyn Query>> {
    let dsl = match dsl.as_object() {
        Some(o) if o.len() == 1 && o.contains_key("query") => &o["query"],
        _ => dsl,
    };
    Compiler {
        index,
        schema: index.schema(),
        ignore_unmapped,
    }
    .query(dsl)
}

struct Compiler<'a> {
    index: &'a Index,
    schema: Schema,
    ignore_unmapped: bool,
}

/// A field, or a path inside a JSON field (`attributes.contractor`).
struct Target {
    field: Field,
    path: Option<String>,
}

/// `params[key]`, or `params` itself in the short form (`{"field": value}`).
fn short<'v>(params: &'v Value, key: &str) -> &'v Value {
    match params.as_object() {
        Some(o) => o.get(key).unwrap_or(params),
        None => params,
    }
}

fn text(v: &Value) -> Result<String> {
    Ok(match v {
        Value::String(s) => s.clone(),
        Value::Number(_) | Value::Bool(_) => v.to_string(),
        _ => bail!("expected text, got {v}"),
    })
}

fn boosted(query: Box<dyn Query>, params: &Value) -> Box<dyn Query> {
    match params.get("boost").and_then(Value::as_f64) {
        Some(b) if b != 1.0 => Box::new(BoostQuery::new(query, b as f32)),
        _ => query,
    }
}

/// Unsupported parameters must not be silently ignored: in particular a
/// misspelled filter must never turn into a match-all query.
fn parameters(body: &Value, kind: &str, allowed: &[&str]) -> Result<()> {
    let obj = body
        .as_object()
        .with_context(|| format!("{kind} needs an object"))?;
    for (name, value) in obj {
        anyhow::ensure!(
            allowed.contains(&name.as_str()) || name == "boost" || name == "_name",
            "unsupported {kind} parameter {name:?}"
        );
        let valid = match name.as_str() {
            "boost" => value
                .as_f64()
                .is_some_and(|n| n >= 0.0 && n <= f32::MAX as f64),
            "tie_breaker" => value.as_f64().is_some_and(|n| (0.0..=1.0).contains(&n)),
            "operator" | "default_operator" => value
                .as_str()
                .is_some_and(|s| s.eq_ignore_ascii_case("and") || s.eq_ignore_ascii_case("or")),
            "slop" | "max_expansions" => value
                .as_u64()
                .is_some_and(|n| n <= u32::MAX as u64 && (name != "max_expansions" || n > 0)),
            "fuzzy_transpositions" | "transpositions" | "case_insensitive" => value.is_boolean(),
            "fields" => value
                .as_array()
                .is_some_and(|a| !a.is_empty() && a.iter().all(Value::is_string)),
            "type" | "field" | "default_field" | "_name" => value.is_string(),
            _ => true,
        };
        anyhow::ensure!(valid, "invalid {kind} parameter {name:?}: {value}");
    }
    Ok(())
}

/// A clause, or a list of them.
fn clauses(v: Option<&Value>) -> Vec<&Value> {
    match v {
        None => Vec::new(),
        Some(Value::Array(a)) => a.iter().collect(),
        Some(one) => vec![one],
    }
}

/// Elasticsearch's minimum_should_match, for `n` optional clauses: 2, -1,
/// "75%", "-25%".
fn minimum(spec: &Value, n: usize) -> Result<usize> {
    let n = n as i128;
    let m = match spec {
        Value::Number(k) => {
            let k = k.as_i64().context("minimum_should_match")? as i128;
            if k < 0 {
                n + k
            } else {
                k
            }
        }
        Value::String(s) => match s.strip_suffix('%') {
            Some(p) => {
                let p = p.trim().parse::<i64>().context("minimum_should_match")? as i128;
                if p < 0 {
                    n - n * -p / 100
                } else {
                    n * p / 100
                }
            }
            None => return minimum(&Value::from(s.trim().parse::<i64>()?), n as usize),
        },
        _ => bail!("minimum_should_match: {spec}"),
    };
    Ok(m.clamp(0, n) as usize)
}

/// Edit distance for `fuzziness`: 0, 1, 2 or "AUTO[:low,high]" by length.
fn distance(spec: &Value, word: &str) -> Result<u8> {
    let len = word.chars().count();
    let auto = |low: usize, high: usize| {
        if len < low {
            0
        } else if len < high {
            1
        } else {
            2
        }
    };
    match spec {
        Value::Number(n) => {
            let n = n.as_u64().context("fuzziness")?;
            anyhow::ensure!(n <= 2, "fuzziness must be 0, 1, 2 or AUTO");
            Ok(n as u8)
        }
        Value::String(s) if s.eq_ignore_ascii_case("auto") => Ok(auto(3, 6)),
        Value::String(s) => match s.to_ascii_lowercase().strip_prefix("auto:") {
            Some(range) => {
                let (low, high) = range.split_once(',').context("fuzziness AUTO:low,high")?;
                let (low, high) = (low.trim().parse()?, high.trim().parse()?);
                anyhow::ensure!(low <= high, "fuzziness AUTO needs low <= high");
                Ok(auto(low, high))
            }
            None => distance(&Value::from(s.parse::<u8>().context("fuzziness")?), word),
        },
        _ => bail!("fuzziness: {spec}"),
    }
}

/// A wildcard pattern (`*`, `?`, `\` to escape) as a regular expression.
fn wildcard_regex(pattern: &str) -> Result<String> {
    let mut out = String::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            '\\' => {
                let escaped = chars.next().context("wildcard has a trailing escape")?;
                out.push_str(&regex_syntax::escape(&escaped.to_string()));
            }
            c => out.push_str(&regex_syntax::escape(&c.to_string())),
        }
    }
    Ok(out)
}

/// A query that scores even where the collector does not ask for scores:
/// tantivy's more-like-this picks its terms by their statistics.
#[derive(Debug)]
struct NeedsScoring(Box<dyn Query>);

impl Clone for NeedsScoring {
    fn clone(&self) -> Self {
        NeedsScoring(self.0.box_clone())
    }
}

impl Query for NeedsScoring {
    fn weight(&self, enable: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        match enable {
            EnableScoring::Disabled {
                searcher_opt: Some(searcher),
                ..
            } => self
                .0
                .weight(EnableScoring::enabled_from_searcher(searcher)),
            other => self.0.weight(other),
        }
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        self.0.query_terms(visitor)
    }
}

impl Compiler<'_> {
    fn query(&self, q: &Value) -> Result<Box<dyn Query>> {
        let obj = q
            .as_object()
            .ok_or_else(|| anyhow!("a query is a JSON object, got {q}"))?;
        let mut entries = obj.iter();
        let (Some((kind, body)), None) = (entries.next(), entries.next()) else {
            bail!(
                "a query has one key, its type; got {:?}",
                obj.keys().collect::<Vec<_>>()
            );
        };
        match kind.as_str() {
            "match_all" | "match_none" => {
                parameters(body, kind, &[])?;
                Ok(boosted(
                    if kind == "match_all" {
                        Box::new(AllQuery)
                    } else {
                        Box::new(EmptyQuery)
                    },
                    body,
                ))
            }
            "bool" => self.boolean(body),
            "constant_score" => {
                parameters(body, kind, &["filter"])?;
                let filter = self.query(
                    body.get("filter")
                        .context("constant_score needs a filter")?,
                )?;
                let boost = body.get("boost").and_then(Value::as_f64).unwrap_or(1.0);
                Ok(Box::new(ConstScoreQuery::new(filter, boost as f32)))
            }
            "dis_max" => {
                parameters(body, kind, &["queries", "tie_breaker"])?;
                anyhow::ensure!(
                    body.get("queries")
                        .and_then(Value::as_array)
                        .is_some_and(|a| !a.is_empty()),
                    "dis_max needs a nonempty queries array"
                );
                let queries = clauses(body.get("queries"))
                    .into_iter()
                    .map(|q| self.query(q))
                    .collect::<Result<Vec<_>>>()?;
                let tie = body
                    .get("tie_breaker")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
                Ok(boosted(
                    Box::new(DisjunctionMaxQuery::with_tie_breaker(queries, tie as f32)),
                    body,
                ))
            }
            "multi_match" => self.multi_match(body),
            "more_like_this" => self.more_like_this(body),
            "exists" => Ok(boosted(self.exists(body)?, body)),
            "query_string" | "simple_query_string" => {
                self.query_string(body, kind == "query_string")
            }
            "term"
            | "terms"
            | "match"
            | "match_phrase"
            | "match_phrase_prefix"
            | "prefix"
            | "wildcard"
            | "regexp"
            | "fuzzy"
            | "range" => {
                let (name, target, params) = self.field_body(kind, body)?;
                let allowed: &[&str] = match kind.as_str() {
                    "term" | "prefix" | "wildcard" | "regexp" => &["value", "case_insensitive"],
                    "match" => &[
                        "query",
                        "operator",
                        "minimum_should_match",
                        "fuzziness",
                        "fuzzy_transpositions",
                    ],
                    "match_phrase" => &["query", "slop"],
                    "match_phrase_prefix" => &["query", "max_expansions"],
                    "fuzzy" => &["value", "fuzziness", "transpositions"],
                    "range" => &["gt", "gte", "lt", "lte"],
                    _ => &[],
                };
                if params.is_object() || kind == "range" {
                    parameters(params, kind, allowed)?;
                }
                if kind == "terms" {
                    parameters(body, kind, &[name])?;
                }
                // A field the split lacks, where that is allowed.
                let Some(target) = target else {
                    return Ok(Box::new(EmptyQuery));
                };
                let case_insensitive = params
                    .get("case_insensitive")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if matches!(kind.as_str(), "prefix" | "wildcard" | "regexp" | "fuzzy") {
                    anyhow::ensure!(
                        self.is_text(&target),
                        "{kind} needs an indexed text field, or a path in a JSON field with text indexing"
                    );
                }
                if kind == "match_phrase_prefix" {
                    anyhow::ensure!(
                        target.path.is_none() && self.is_text(&target),
                        "match_phrase_prefix needs an indexed text field, not a path inside a JSON field"
                    );
                }
                if kind == "term" && case_insensitive {
                    anyhow::ensure!(
                        self.is_text(&target),
                        "case_insensitive needs a text field, or a path in a JSON field with text indexing"
                    );
                }
                let q = match kind.as_str() {
                    "term" if case_insensitive => {
                        let value = text(short(params, "value"))?;
                        self.pattern(&target, &regex_syntax::escape(&value), true)?
                    }
                    "term" => Box::new(TermQuery::new(
                        self.term(&target, short(params, "value"))?,
                        IndexRecordOption::WithFreqs,
                    )) as Box<dyn Query>,
                    "terms" => {
                        let values = params.as_array().context("terms takes a list of values")?;
                        let terms = values
                            .iter()
                            .map(|v| self.term(&target, v))
                            .collect::<Result<Vec<_>>>()?;
                        Box::new(TermSetQuery::new(terms))
                    }
                    "match" => self.match_query(&target, params)?,
                    "match_phrase" => self.phrase(&target, params, false)?,
                    "match_phrase_prefix" => self.phrase(&target, params, true)?,
                    "prefix" => {
                        let prefix = regex_syntax::escape(&text(short(params, "value"))?);
                        self.pattern(&target, &format!("{prefix}.*"), case_insensitive)?
                    }
                    "wildcard" => {
                        let pattern = wildcard_regex(&text(short(params, "value"))?)?;
                        self.pattern(&target, &pattern, case_insensitive)?
                    }
                    "regexp" => {
                        let pattern = text(short(params, "value"))?;
                        self.pattern(&target, &pattern, case_insensitive)?
                    }
                    "fuzzy" => {
                        let value = text(short(params, "value"))?;
                        let d = distance(
                            params.get("fuzziness").unwrap_or(&Value::from("AUTO")),
                            &value,
                        )?;
                        let transpositions = params
                            .get("transpositions")
                            .and_then(Value::as_bool)
                            .unwrap_or(true);
                        Box::new(FuzzyTermQuery::new(
                            self.term(&target, &Value::from(value))?,
                            d,
                            transpositions,
                        ))
                    }
                    _ => self.range(&target, params)?,
                };
                Ok(if kind == "terms" {
                    boosted(q, body)
                } else {
                    boosted(q, params)
                })
            }
            other => bail!("unsupported query type {other:?}"),
        }
    }

    /// Whether the target's terms are text: an indexed text field, or a path in
    /// a JSON field that indexes text.
    fn is_text(&self, target: &Target) -> bool {
        let entry = self.schema.get_field_entry(target.field);
        match (entry.field_type(), &target.path) {
            (FieldType::Str(_), None) => entry.is_indexed(),
            (FieldType::JsonObject(options), Some(_)) => {
                options.get_text_indexing_options().is_some()
            }
            _ => false,
        }
    }

    /// A regular expression over the target's terms.
    fn pattern(
        &self,
        target: &Target,
        regex: &str,
        case_insensitive: bool,
    ) -> Result<Box<dyn Query>> {
        let json = match (
            self.schema.get_field_entry(target.field).field_type(),
            &target.path,
        ) {
            (FieldType::JsonObject(options), Some(path)) => Some((path.as_str(), options)),
            _ => None,
        };
        Ok(Box::new(PatternQuery::new(
            target.field,
            json,
            regex,
            case_insensitive,
        )?))
    }

    /// `{"<field>": params}` of a field-level query: the field's name, what it
    /// names (nothing, if it is missing and that is allowed) and the params.
    fn field_body<'v>(
        &self,
        kind: &str,
        body: &'v Value,
    ) -> Result<(&'v str, Option<Target>, &'v Value)> {
        let obj = body
            .as_object()
            .with_context(|| format!("{kind} takes {{\"<field>\": ...}}"))?;
        let mut fields = obj
            .iter()
            .filter(|(k, _)| kind != "terms" || (k.as_str() != "boost" && k.as_str() != "_name"));
        let (Some((name, params)), None) = (fields.next(), fields.next()) else {
            bail!("{kind} takes one field");
        };
        Ok((name.as_str(), self.resolve(name)?, params))
    }

    /// A field, or a path inside a JSON field. `None` for a field the schema
    /// lacks, if `ignore_unmapped`.
    fn resolve(&self, name: &str) -> Result<Option<Target>> {
        match self.schema.find_field(name) {
            Some((field, "")) => Ok(Some(Target { field, path: None })),
            Some((field, path))
                if matches!(
                    self.schema.get_field_entry(field).field_type(),
                    FieldType::JsonObject(_)
                ) =>
            {
                Ok(Some(Target {
                    field,
                    path: Some(path.to_owned()),
                }))
            }
            _ if self.ignore_unmapped => Ok(None),
            _ => bail!("no field {name:?}"),
        }
    }

    /// Fields by name; those the schema lacks are left out if `ignore_unmapped`.
    fn fields(&self, names: &[&str]) -> Result<Vec<Field>> {
        let mut found = Vec::new();
        for name in names {
            match self.schema.get_field(name) {
                Ok(field) => found.push(field),
                Err(_) if self.ignore_unmapped => {}
                Err(_) => bail!("no field {name:?}"),
            }
        }
        Ok(found)
    }

    /// A value as a term of a field or of a path in a JSON field.
    fn term(&self, target: &Target, v: &Value) -> Result<Term> {
        match &target.path {
            None => self.field_term(target.field, v),
            Some(path) => self.json_term(target.field, path, v),
        }
    }

    /// A JSON value as the term a JSON field indexes at `path`: text as it
    /// is (not analyzed), numbers by their JSON type, booleans.
    fn json_term(&self, field: Field, path: &str, v: &Value) -> Result<Term> {
        let entry = self.schema.get_field_entry(field);
        let FieldType::JsonObject(options) = entry.field_type() else {
            bail!("{}: not a JSON field", entry.name());
        };
        let mut term = Term::from_field_json_path(field, path, options.is_expand_dots_enabled());
        match v {
            Value::String(s) => term.append_type_and_str(s),
            Value::Bool(b) => term.append_type_and_fast_value(*b),
            Value::Number(n) => match (n.as_i64(), n.as_u64()) {
                (Some(i), _) => term.append_type_and_fast_value(i),
                (None, Some(u)) => term.append_type_and_fast_value(u),
                _ => term.append_type_and_fast_value(n.as_f64().context("a number")?),
            },
            _ => bail!("{}.{path}: cannot use {v} as a value", entry.name()),
        }
        Ok(term)
    }

    /// A field value as a term: text verbatim (not analyzed), numbers, dates
    /// (RFC 3339 or epoch milliseconds), booleans, IP addresses, facets.
    fn field_term(&self, field: Field, v: &Value) -> Result<Term> {
        let entry = self.schema.get_field_entry(field);
        let name = entry.name();
        let bad = || {
            anyhow!(
                "{name}: cannot use {v} as a {:?} value",
                entry.field_type().value_type()
            )
        };
        Ok(match entry.field_type() {
            FieldType::Str(_) => Term::from_field_text(field, &text(v)?),
            FieldType::Facet(_) => Term::from_facet(field, &Facet::from(text(v)?.as_str())),
            FieldType::I64(_) => Term::from_field_i64(
                field,
                v.as_i64()
                    .or_else(|| v.as_str()?.parse().ok())
                    .ok_or_else(bad)?,
            ),
            FieldType::U64(_) => Term::from_field_u64(
                field,
                v.as_u64()
                    .or_else(|| v.as_str()?.parse().ok())
                    .ok_or_else(bad)?,
            ),
            FieldType::F64(_) => Term::from_field_f64(
                field,
                v.as_f64()
                    .or_else(|| v.as_str()?.parse().ok())
                    .ok_or_else(bad)?,
            ),
            FieldType::Bool(_) => Term::from_field_bool(
                field,
                v.as_bool()
                    .or_else(|| v.as_str()?.parse().ok())
                    .ok_or_else(bad)?,
            ),
            FieldType::Date(_) => {
                let date = match v {
                    Value::Number(n) => DateTime::from_timestamp_nanos(
                        n.as_i64()
                            .and_then(|n| n.checked_mul(1_000_000))
                            .ok_or_else(bad)?,
                    ),
                    Value::String(s) => {
                        use tantivy::time::format_description::well_known::Rfc3339;
                        let date =
                            tantivy::time::OffsetDateTime::parse(s, &Rfc3339).map_err(|_| bad())?;
                        DateTime::from_timestamp_nanos(
                            date.unix_timestamp_nanos().try_into().map_err(|_| bad())?,
                        )
                    }
                    _ => return Err(bad()),
                };
                Term::from_field_date_for_search(field, date)
            }
            FieldType::IpAddr(_) => {
                let ip: std::net::IpAddr = text(v)?.parse().map_err(|_| bad())?;
                let ip = match ip {
                    std::net::IpAddr::V4(v4) => v4.to_ipv6_mapped(),
                    std::net::IpAddr::V6(v6) => v6,
                };
                Term::from_field_ip_addr(field, ip)
            }
            _ => bail!("{name}: term queries on this field type are not supported"),
        })
    }

    /// The field's analyzer applied to `text`: (position, term, text) per token.
    /// Values that are not analyzed text give the value itself.
    fn tokens(&self, target: &Target, v: &Value) -> Result<Vec<(usize, Term, String)>> {
        let analyzed = match (
            &target.path,
            self.schema.get_field_entry(target.field).field_type(),
        ) {
            (None, FieldType::Str(_)) => true,
            (Some(_), _) => v.is_string(),
            _ => false,
        };
        if !analyzed {
            return Ok(vec![(0, self.term(target, v)?, text(v)?)]);
        }
        let mut analyzer = self.index.tokenizer_for_field(target.field)?;
        let text = text(v)?;
        let mut stream = analyzer.token_stream(&text);
        let mut out = Vec::new();
        while stream.advance() {
            let token = stream.token();
            let term = match &target.path {
                None => Term::from_field_text(target.field, &token.text),
                Some(path) => {
                    self.json_term(target.field, path, &Value::from(token.text.clone()))?
                }
            };
            out.push((token.position, term, token.text.clone()));
        }
        Ok(out)
    }

    fn match_query(&self, target: &Target, params: &Value) -> Result<Box<dyn Query>> {
        let tokens = self.tokens(target, short(params, "query"))?;
        let fuzziness = params.get("fuzziness");
        let transpositions = params
            .get("fuzzy_transpositions")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let clauses = tokens
            .into_iter()
            .map(|(_, term, word)| {
                Ok(match fuzziness {
                    Some(f) => Box::new(FuzzyTermQuery::new(
                        term,
                        distance(f, &word)?,
                        transpositions,
                    )) as Box<dyn Query>,
                    None => Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs)),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if clauses.is_empty() {
            return Ok(Box::new(EmptyQuery)); // e.g. only stop words
        }
        let and = params
            .get("operator")
            .and_then(Value::as_str)
            .is_some_and(|o| o.eq_ignore_ascii_case("and"));
        Ok(if and {
            Box::new(BooleanQuery::intersection(clauses))
        } else if let Some(m) = params.get("minimum_should_match") {
            let n = minimum(m, clauses.len())?;
            Box::new(BooleanQuery::union_with_minimum_required_clauses(
                clauses, n,
            ))
        } else {
            Box::new(BooleanQuery::union(clauses))
        })
    }

    fn phrase(&self, target: &Target, params: &Value, prefix: bool) -> Result<Box<dyn Query>> {
        let tokens: Vec<(usize, Term)> = self
            .tokens(target, short(params, "query"))?
            .into_iter()
            .map(|(position, term, _)| (position, term))
            .collect();
        Ok(match tokens.len() {
            0 => Box::new(EmptyQuery),
            1 if !prefix => Box::new(TermQuery::new(
                tokens[0].1.clone(),
                IndexRecordOption::WithFreqs,
            )),
            _ if prefix => {
                let mut q = PhrasePrefixQuery::new_with_offset(tokens);
                if let Some(n) = params.get("max_expansions").and_then(Value::as_u64) {
                    q.set_max_expansions(n as u32);
                }
                Box::new(q)
            }
            _ => {
                let slop = params.get("slop").and_then(Value::as_u64).unwrap_or(0) as u32;
                Box::new(PhraseQuery::new_with_offset_and_slop(tokens, slop))
            }
        })
    }

    fn range(&self, target: &Target, params: &Value) -> Result<Box<dyn Query>> {
        anyhow::ensure!(
            params.get("gte").is_some()
                || params.get("gt").is_some()
                || params.get("lte").is_some()
                || params.get("lt").is_some(),
            "range needs at least one bound"
        );
        anyhow::ensure!(
            !(params.get("gte").is_some() && params.get("gt").is_some())
                && !(params.get("lte").is_some() && params.get("lt").is_some()),
            "range has conflicting bounds"
        );
        let bound = |inclusive: &str, exclusive: &str| -> Result<Bound<Term>> {
            Ok(match (params.get(inclusive), params.get(exclusive)) {
                (Some(v), _) => Bound::Included(self.term(target, v)?),
                (None, Some(v)) => Bound::Excluded(self.term(target, v)?),
                (None, None) => Bound::Unbounded,
            })
        };
        Ok(Box::new(RangeQuery::new(
            bound("gte", "gt")?,
            bound("lte", "lt")?,
        )))
    }

    fn boolean(&self, body: &Value) -> Result<Box<dyn Query>> {
        parameters(
            body,
            "bool",
            &[
                "must",
                "should",
                "must_not",
                "filter",
                "minimum_should_match",
            ],
        )?;
        let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        for q in clauses(body.get("must")) {
            subqueries.push((Occur::Must, self.query(q)?));
        }
        for q in clauses(body.get("filter")) {
            subqueries.push((
                Occur::Must,
                Box::new(ConstScoreQuery::new(self.query(q)?, 0.0)),
            ));
        }
        for q in clauses(body.get("must_not")) {
            subqueries.push((Occur::MustNot, self.query(q)?));
        }
        let should = clauses(body.get("should"));
        let required = !subqueries.iter().any(|(o, _)| *o == Occur::Must);
        for q in &should {
            subqueries.push((Occur::Should, self.query(q)?));
        }
        // Only must_not: everything else.
        if subqueries.iter().all(|(o, _)| *o == Occur::MustNot) && !subqueries.is_empty() {
            subqueries.push((
                Occur::Must,
                Box::new(ConstScoreQuery::new(Box::new(AllQuery), 0.0)),
            ));
        }
        let minimum = body
            .get("minimum_should_match")
            .map(|m| minimum(m, should.len()))
            .transpose()?;
        let q: Box<dyn Query> = match minimum {
            _ if subqueries.is_empty() => Box::new(AllQuery),
            Some(m) => Box::new(BooleanQuery::with_minimum_required_clauses(subqueries, m)),
            // As in Elasticsearch, a should clause is required only without must or filter.
            None if required && !should.is_empty() => {
                Box::new(BooleanQuery::with_minimum_required_clauses(subqueries, 1))
            }
            None => Box::new(BooleanQuery::new(subqueries)),
        };
        Ok(boosted(q, body))
    }

    fn multi_match(&self, body: &Value) -> Result<Box<dyn Query>> {
        parameters(
            body,
            "multi_match",
            &[
                "query",
                "fields",
                "type",
                "operator",
                "minimum_should_match",
                "fuzziness",
                "fuzzy_transpositions",
                "slop",
                "max_expansions",
                "tie_breaker",
            ],
        )?;
        let fields: Vec<(Option<Target>, f32)> = match body.get("fields") {
            Some(Value::Array(names)) => names
                .iter()
                .map(|n| {
                    let n = n.as_str().context("multi_match fields are names")?;
                    let (name, boost) = match n.split_once('^') {
                        Some((name, b)) => (name, b.parse::<f32>().context("field boost")?),
                        None => (n, 1.0),
                    };
                    anyhow::ensure!(boost.is_finite() && boost >= 0.0, "invalid field boost");
                    Ok((self.resolve(name)?, boost))
                })
                .collect::<Result<_>>()?,
            None => crate::search::text_fields(&self.schema)
                .into_iter()
                .map(|f| {
                    (
                        Some(Target {
                            field: f,
                            path: None,
                        }),
                        1.0,
                    )
                })
                .collect(),
            Some(other) => bail!("multi_match fields: {other}"),
        };
        let kind = body
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("best_fields");
        let fields: Vec<(Target, f32)> = fields
            .into_iter()
            .filter_map(|(target, boost)| Some((target?, boost)))
            .collect();
        if fields.is_empty() {
            return Ok(Box::new(EmptyQuery));
        }
        let per_field = fields
            .into_iter()
            .map(|(target, boost)| {
                let q = match kind {
                    "best_fields" | "most_fields" => self.match_query(&target, body)?,
                    "phrase" => self.phrase(&target, body, false)?,
                    "phrase_prefix" => self.phrase(&target, body, true)?,
                    other => bail!("unsupported multi_match type {other:?}"),
                };
                Ok(if boost != 1.0 {
                    Box::new(BoostQuery::new(q, boost)) as Box<dyn Query>
                } else {
                    q
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let q: Box<dyn Query> = if kind == "most_fields" {
            Box::new(BooleanQuery::union(per_field))
        } else {
            let tie = body
                .get("tie_breaker")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            Box::new(DisjunctionMaxQuery::with_tie_breaker(per_field, tie as f32))
        };
        Ok(boosted(q, body))
    }

    /// `{"more_like_this": {"like": "text", "fields": [...]}}`: documents like
    /// a text, by the terms that mark it out from the others. (Not a document
    /// of the index: fetch its text, and pass that.)
    fn more_like_this(&self, body: &Value) -> Result<Box<dyn Query>> {
        parameters(
            body,
            "more_like_this",
            &[
                "like",
                "fields",
                "min_term_freq",
                "max_query_terms",
                "min_doc_freq",
                "max_doc_freq",
                "min_word_length",
                "max_word_length",
                "boost_terms",
                "stop_words",
            ],
        )?;
        let like: Vec<OwnedValue> = match body.get("like") {
            Some(Value::String(text)) => vec![OwnedValue::Str(text.clone())],
            Some(Value::Array(texts)) if !texts.is_empty() => texts
                .iter()
                .map(|t| {
                    t.as_str()
                        .map(|t| OwnedValue::Str(t.to_owned()))
                        .context("more_like_this: like takes text")
                })
                .collect::<Result<_>>()?,
            _ => bail!("more_like_this needs `like`: a text, or a list of texts"),
        };
        let is_text = |field: Field| {
            matches!(
                self.schema.get_field_entry(field).field_type(),
                FieldType::Str(_)
            )
        };
        let fields: Vec<Field> = match body.get("fields") {
            Some(Value::Array(names)) => {
                let mut found = Vec::new();
                for name in names {
                    let name = name.as_str().context("more_like_this: fields are names")?;
                    if let Some(target) = self.resolve(name)? {
                        anyhow::ensure!(
                            target.path.is_none() && is_text(target.field),
                            "more_like_this: {name:?} is not a text field"
                        );
                        found.push(target.field);
                    }
                }
                found
            }
            _ => crate::search::text_fields(&self.schema)
                .into_iter()
                .filter(|f| is_text(*f))
                .collect(),
        };
        if fields.is_empty() {
            return Ok(Box::new(EmptyQuery));
        }
        let number = |key: &str| -> Result<Option<u64>> {
            body.get(key)
                .map(|v| {
                    v.as_u64()
                        .with_context(|| format!("more_like_this: {key} is a count"))
                })
                .transpose()
        };
        let mut builder = MoreLikeThisQuery::builder();
        if let Some(n) = number("min_term_freq")? {
            builder = builder.with_min_term_frequency(n as usize);
        }
        if let Some(n) = number("max_query_terms")? {
            builder = builder.with_max_query_terms(n as usize);
        }
        if let Some(n) = number("min_doc_freq")? {
            builder = builder.with_min_doc_frequency(n);
        }
        if let Some(n) = number("max_doc_freq")? {
            builder = builder.with_max_doc_frequency(n);
        }
        if let Some(n) = number("min_word_length")? {
            builder = builder.with_min_word_length(n as usize);
        }
        if let Some(n) = number("max_word_length")? {
            builder = builder.with_max_word_length(n as usize);
        }
        if let Some(boost) = body.get("boost_terms") {
            let boost = boost
                .as_f64()
                .filter(|b| *b >= 0.0 && *b <= f32::MAX as f64);
            builder =
                builder.with_boost_factor(boost.context("more_like_this: boost_terms")? as f32);
        }
        if let Some(words) = body.get("stop_words") {
            let words = words
                .as_array()
                .context("more_like_this: stop_words is a list of words")?
                .iter()
                .map(|w| {
                    w.as_str()
                        .map(str::to_owned)
                        .context("more_like_this: stop_words")
                })
                .collect::<Result<Vec<_>>>()?;
            builder = builder.with_stop_words(words);
        }
        let document = fields.into_iter().map(|f| (f, like.clone())).collect();
        let query = builder.with_document_fields(document);
        Ok(boosted(Box::new(NeedsScoring(Box::new(query))), body))
    }

    /// `{"exists": {"field": "<name>"}}`: a fast field's column says which
    /// documents have a value. For an indexed text field without one, any term
    /// in the field's dictionary does, which is slower. A JSON field matches
    /// by any path inside it.
    fn exists(&self, body: &Value) -> Result<Box<dyn Query>> {
        parameters(body, "exists", &["field"])?;
        let name = body
            .get("field")
            .and_then(Value::as_str)
            .context("exists takes {\"field\": \"<name>\"}")?;
        let Some(target) = self.resolve(name)? else {
            return Ok(Box::new(EmptyQuery));
        };
        let entry = self.schema.get_field_entry(target.field);
        if entry.is_fast() {
            let subpaths =
                target.path.is_none() && matches!(entry.field_type(), FieldType::JsonObject(_));
            return Ok(Box::new(ExistsQuery::new(name.to_owned(), subpaths)));
        }
        anyhow::ensure!(
            target.path.is_none()
                && entry.is_indexed()
                && matches!(entry.field_type(), FieldType::Str(_)),
            "exists needs a fast field, or an indexed text field: {name:?} is neither"
        );
        self.pattern(&target, "(?s).*", false)
    }

    fn query_string(&self, body: &Value, strict: bool) -> Result<Box<dyn Query>> {
        parameters(
            body,
            if strict {
                "query_string"
            } else {
                "simple_query_string"
            },
            &["query", "fields", "default_field", "default_operator"],
        )?;
        let text = text(short(body, "query"))?;
        let named: Option<Vec<&str>> = match (body.get("fields"), body.get("default_field")) {
            (Some(Value::Array(names)), _) => Some(
                names
                    .iter()
                    .map(|n| n.as_str().context("field names"))
                    .collect::<Result<_>>()?,
            ),
            (_, Some(Value::String(name))) => Some(vec![name.as_str()]),
            _ => None,
        };
        let fields = match &named {
            Some(names) => self.fields(names)?,
            None => crate::search::text_fields(&self.schema),
        };
        if named.is_some() && fields.is_empty() {
            return Ok(Box::new(EmptyQuery));
        }
        let mut parser = QueryParser::for_index(self.index, fields);
        if body
            .get("default_operator")
            .and_then(Value::as_str)
            .is_some_and(|o| o.eq_ignore_ascii_case("and"))
        {
            parser.set_conjunction_by_default();
        }
        let q = if strict {
            parser.parse_query(&text)?
        } else {
            parser.parse_query_lenient(&text).0
        };
        Ok(boosted(q, body))
    }
}

#[cfg(test)]
mod tests {
    use crate::search::Request;
    use crate::split::testing::split;

    const DOCS: &[&str] = &[
        r#"{"id": 1, "body": "The quick brown fox jumps", "tag": "alpha"}"#,
        r#"{"id": 2, "body": "Small CATS and a café", "tag": ["beta", "gamma"]}"#,
        r#"{"id": 3, "body": "cats chasing the fox", "tag": "alpha"}"#,
        r#"{"id": 4, "body": "a dog", "tag": "delta"}"#,
    ];

    fn ids(query: serde_json::Value) -> Vec<i64> {
        let s = split(DOCS);
        let mut ids: Vec<i64> =
            Request::new(&[&s], &query.to_string(), r#"{"fast": ["id"]}"#, None)
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

    fn fails(query: serde_json::Value) -> String {
        let s = split(DOCS);
        let r = Request::new(&[&s], &query.to_string(), "", None)
            .unwrap()
            .hits();
        format!("{:#}", r.expect_err("query should fail"))
    }

    use serde_json::json;

    #[test]
    fn compiles_leaf_queries() {
        assert_eq!(ids(json!({"match_all": {}})), [1, 2, 3, 4]);
        assert_eq!(ids(json!({"match_none": {}})), Vec::<i64>::new());
        assert_eq!(ids(json!({"query": {"match": {"body": "fox"}}})), [1, 3]);
        assert_eq!(
            ids(json!({"match": {"body": {"query": "cats fox", "operator": "and"}}})),
            [3]
        );
        assert_eq!(
            ids(json!({"match": {"body": {"query": "cats fox dog", "minimum_should_match": 2}}})),
            [3]
        );
        assert_eq!(
            ids(
                json!({"match": {"body": {"query": "cats fox dog", "minimum_should_match": "-1"}}})
            ),
            [3]
        );
        assert_eq!(
            ids(json!({"match": {"body": {"query": "foz", "fuzziness": 1}}})),
            [1, 3]
        );
        assert_eq!(
            ids(json!({"match": {"body": {"query": "foz", "fuzziness": "AUTO"}}})),
            [1, 3]
        );
        assert_eq!(
            ids(json!({"match": {"body": {"query": "fo", "fuzziness": "AUTO"}}})),
            Vec::<i64>::new()
        );
        assert_eq!(ids(json!({"match_phrase": {"body": "brown fox"}})), [1]);
        assert_eq!(
            ids(json!({"match_phrase": {"body": {"query": "quick fox", "slop": 1}}})),
            [1]
        );
        assert_eq!(
            ids(json!({"match_phrase_prefix": {"body": "quick bro"}})),
            [1]
        );
        assert_eq!(ids(json!({"term": {"tag": "alpha"}})), [1, 3]);
        assert_eq!(ids(json!({"term": {"id": {"value": "2"}}})), [2]);
        assert_eq!(ids(json!({"terms": {"tag": ["beta", "delta"]}})), [2, 4]);
        assert_eq!(ids(json!({"prefix": {"tag": "al"}})), [1, 3]);
        assert_eq!(ids(json!({"wildcard": {"tag": {"value": "?amm*"}}})), [2]);
        // Term-level queries are not analyzed; case_insensitive opts out of case.
        assert_eq!(ids(json!({"prefix": {"tag": "AL"}})), Vec::<i64>::new());
        assert_eq!(
            ids(json!({"prefix": {"tag": {"value": "AL", "case_insensitive": true}}})),
            [1, 3]
        );
        assert_eq!(
            ids(json!({"wildcard": {"tag": {"value": "*MM*", "case_insensitive": true}}})),
            [2]
        );
        assert_eq!(
            ids(json!({"regexp": {"tag": {"value": "D.L.A", "case_insensitive": true}}})),
            [4]
        );
        assert_eq!(
            ids(json!({"term": {"tag": {"value": "ALPHA", "case_insensitive": true}}})),
            [1, 3]
        );
        assert_eq!(
            ids(json!({"term": {"tag": {"value": "ALPHA"}}})),
            Vec::<i64>::new()
        );
        // The value is literal: no expression in a prefix or a case-insensitive term.
        assert_eq!(ids(json!({"prefix": {"tag": ".*"}})), Vec::<i64>::new());
        assert_eq!(
            ids(json!({"term": {"tag": {"value": "al.*", "case_insensitive": true}}})),
            Vec::<i64>::new()
        );
        assert_eq!(ids(json!({"regexp": {"tag": "d.l.a"}})), [4]);
        assert_eq!(ids(json!({"fuzzy": {"tag": {"value": "alpah"}}})), [1, 3]);
        assert_eq!(ids(json!({"exists": {"field": "tag"}})), [1, 2, 3, 4]);
        assert_eq!(ids(json!({"range": {"id": {"gt": 1, "lte": 3}}})), [2, 3]);
        assert_eq!(ids(json!({"query_string": {"query": "fox AND cats"}})), [3]);
        assert_eq!(ids(json!({"simple_query_string": {"query": "dog ("}})), [4]);
        assert_eq!(
            ids(json!({"multi_match": {"query": "alpha fox", "fields": ["body", "tag^2"]}})),
            [1, 3]
        );
    }

    #[test]
    fn compiles_compound_queries() {
        let q = json!({"bool": {
            "must": {"match": {"body": "cats"}},
            "filter": [{"term": {"tag": "alpha"}}],
            "must_not": {"term": {"id": 99}},
        }});
        assert_eq!(ids(q), [3]);
        assert_eq!(
            ids(
                json!({"bool": {"should": [{"term": {"tag": "alpha"}}, {"term": {"tag": "delta"}}]}})
            ),
            [1, 3, 4]
        );
        assert_eq!(
            ids(
                json!({"bool": {"should": [{"match": {"body": "fox"}}, {"match": {"body": "cats"}}], "minimum_should_match": 2}})
            ),
            [3]
        );
        // With a must, should clauses only score.
        assert_eq!(
            ids(
                json!({"bool": {"must": {"term": {"tag": "delta"}}, "should": {"match": {"body": "fox"}}}})
            ),
            [4]
        );
        assert_eq!(
            ids(json!({"bool": {"must_not": {"term": {"tag": "alpha"}}}})),
            [2, 4]
        );
        assert_eq!(ids(json!({"bool": {}})), [1, 2, 3, 4]);
        assert_eq!(
            ids(json!({"bool": {"minimum_should_match": 0}})),
            [1, 2, 3, 4]
        );
        assert_eq!(
            ids(json!({"bool": {"should": [
            {"match": {"body": "fox"}}, {"match": {"body": "cats"}}, {"match": {"body": "dog"}}
        ], "minimum_should_match": "75%"}})),
            [3]
        );
        assert_eq!(
            ids(
                json!({"dis_max": {"queries": [{"term": {"tag": "delta"}}, {"match": {"body": "fox"}}]}})
            ),
            [1, 3, 4]
        );
        assert_eq!(
            ids(json!({"constant_score": {"filter": {"term": {"tag": "alpha"}}, "boost": 2}})),
            [1, 3]
        );
        // A filter does not score; constant_score scores its boost.
        let s = split(DOCS);
        let scores = |q: serde_json::Value| -> Vec<f32> {
            Request::new(&[&s], &q.to_string(), "", None)
                .unwrap()
                .hits()
                .unwrap()
                .into_iter()
                .map(|h| h.score)
                .collect()
        };
        assert_eq!(
            scores(json!({"bool": {"filter": {"term": {"tag": "alpha"}}}})),
            [0.0, 0.0]
        );
        assert_eq!(
            scores(json!({"bool": {"must_not": {"term": {"tag": "alpha"}}}})),
            [0.0, 0.0]
        );
        assert_eq!(
            scores(json!({"constant_score": {"filter": {"term": {"tag": "alpha"}}, "boost": 2}})),
            [2.0, 2.0]
        );
    }

    #[test]
    fn finds_documents_like_a_text() {
        let like = |extra: serde_json::Value| {
            let mut body = json!({"like": "cats fox", "fields": ["body"], "min_term_freq": 1, "min_doc_freq": 1});
            body.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            json!({"more_like_this": body})
        };
        // Cats are in 2 and 3, foxes in 1 and 3.
        assert_eq!(ids(like(json!({}))), [1, 2, 3]);
        // Stop words are compared with the analyzed terms: "cat", as the index stems.
        assert_eq!(ids(like(json!({"stop_words": ["cat"]}))), [1, 3]);
        assert_eq!(ids(like(json!({"stop_words": ["cats"]}))), [1, 2, 3]);
        assert_eq!(ids(like(json!({"like": ["lazy dog", "fox"]}))), [1, 3, 4]); // dog is in 4
        assert_eq!(ids(like(json!({"min_doc_freq": 3}))), Vec::<i64>::new()); // no term is common enough
        assert!(ids(like(json!({"max_query_terms": 1, "boost_terms": 2.0}))).len() >= 2);
        assert_eq!(ids(like(json!({"fields": ["tag"]}))), Vec::<i64>::new()); // no such terms in tag
                                                                              // A count asks for no scores, which this query needs.
        let s = split(DOCS);
        let count = |q: serde_json::Value| {
            Request::new(&[&s], &q.to_string(), "", None)
                .unwrap()
                .count()
        };
        assert_eq!(count(like(json!({}))).unwrap(), 3);
        assert_eq!(
            count(json!({"bool": {"must": like(json!({})), "filter": {"term": {"tag": "alpha"}}}}))
                .unwrap(),
            2
        );
        assert!(fails(json!({"more_like_this": {"fields": ["body"]}})).contains("needs `like`"));
        assert!(fails(json!({"more_like_this": {"like": 5}})).contains("needs `like`"));
        assert!(
            fails(json!({"more_like_this": {"like": "fox", "fields": ["id"]}}))
                .contains("not a text field")
        );
        assert!(
            fails(json!({"more_like_this": {"like": "fox", "fields": ["nosuch"]}}))
                .contains("no field")
        );
        assert!(
            fails(json!({"more_like_this": {"like": "fox", "min_term_freq": -1}}))
                .contains("is a count")
        );
        assert!(
            fails(json!({"more_like_this": {"like": "fox", "minimum_should_match": 1}}))
                .contains("unsupported")
        );
        assert!(
            fails(json!({"more_like_this": {"like": "fox", "boost_terms": "x"}}))
                .contains("boost_terms")
        );
    }

    #[test]
    fn rejects_what_it_does_not_support() {
        assert!(fails(json!({"nested": {"path": "x"}})).contains("unsupported query type"));
        assert!(fails(json!({"match": {"nosuch": "x"}})).contains("no field"));
        assert!(fails(json!({"term": {"id": "x"}})).contains("cannot use"));
        assert!(fails(json!({"match": {"body": "x"}, "term": {"id": 1}})).contains("one key"));
        assert!(!fails(json!({"query_string": {"query": "body:("}})).is_empty());
        for q in [
            json!({"bool": {"filters": {"term": {"id": 1}}}}),
            json!({"bool": null}),
            json!({"match_all": {"filter": {"term": {"id": 1}}}}),
            json!({"match": {"body": {"query": "fox", "operator": "xor"}}}),
            json!({"match": {"body": {"query": "fox", "fuzziness": 3}}}),
            json!({"prefix": {"tag": {"value": "a", "case_insensitive": "yes"}}}),
            json!({"term": {"id": {"value": 1, "case_insensitive": true}}}),
            json!({"fuzzy": {"tag": {"value": "a", "case_insensitive": true}}}),
            json!({"regexp": {"tag": "("}}),
            json!({"match_phrase_prefix": {"body": "quick bro", "case_insensitive": true}}),
            json!({"prefix": {"id": "1"}}),
            json!({"range": {"id": {}}}),
            json!({"range": {"id": {"gt": 1, "gte": 2}}}),
            json!({"term": {"id": {"value": 1, "boost": "bad"}}}),
            json!({"dis_max": {"queries": []}}),
            json!({"match_phrase": {"body": {"query": "fox", "slop": -1}}}),
            json!({"wildcard": {"tag": "a\\"}}),
        ] {
            assert!(!fails(q).is_empty());
        }
        assert_eq!(
            ids(json!({"wildcard": {"tag": "al\\*"}})),
            Vec::<i64>::new()
        );
        assert_eq!(
            ids(json!({"wildcard": {"tag": "*@*.com"}})),
            Vec::<i64>::new()
        );
        assert_eq!(
            super::minimum(&json!("-9223372036854775808%"), 3).unwrap(),
            0
        );
        let s = split(DOCS);
        assert!(Request::new(&[&s], "{not json", "", None)
            .unwrap()
            .hits()
            .is_err());
    }
}

#[cfg(test)]
mod json_tests {
    use crate::search::Request;
    use crate::split::testing::{build_with, open};
    use serde_json::json;
    use tantivy::schema::{Schema, FAST, STORED, TEXT};

    fn schema() -> String {
        let mut b = Schema::builder();
        b.add_i64_field("id", FAST | STORED | tantivy::schema::INDEXED);
        b.add_json_field("meta", TEXT | FAST);
        b.add_text_field("kw", tantivy::schema::STRING);
        b.add_text_field("note", STORED);
        b.add_text_field("body", TEXT);
        serde_json::to_string(&b.build()).unwrap()
    }

    const DOCS: &[&str] = &[
        r#"{"id": 1, "meta": {"color": "red", "n": 5, "flag": true, "tags": "Big roof"}, "kw": "a\nb", "body": "alpha"}"#,
        r#"{"id": 2, "meta": {"color": "blue", "n": 9, "tags": "small roof"}, "kw": "c", "note": "x"}"#,
        r#"{"id": 3, "meta": {"color": "red", "n": 1, "flag": false}, "body": "beta"}"#,
        r#"{"id": 4, "note": "y"}"#,
    ];

    fn ids(query: serde_json::Value, options: &str) -> anyhow::Result<Vec<i64>> {
        let s = open(build_with(&schema(), "", DOCS));
        let options = if options.is_empty() { "{}" } else { options };
        let options = options
            .replacen('{', r#"{"fast": ["id"], "#, 1)
            .replace(", }", "}");
        let hits = Request::new(&[&s], &query.to_string(), &options, None)?.hits()?;
        let mut ids: Vec<i64> = hits
            .into_iter()
            .map(|h| {
                serde_json::from_str::<serde_json::Value>(&h.doc).unwrap()["id"]
                    .as_i64()
                    .unwrap()
            })
            .collect();
        ids.sort();
        Ok(ids)
    }

    fn ok(query: serde_json::Value) -> Vec<i64> {
        ids(query, "").unwrap()
    }

    fn fails(query: serde_json::Value, options: &str) -> String {
        format!("{:#}", ids(query, options).expect_err("should fail"))
    }

    #[test]
    fn reaches_paths_inside_json_fields() {
        assert_eq!(ok(json!({"term": {"meta.color": "red"}})), [1, 3]);
        assert_eq!(ok(json!({"term": {"meta.n": 5}})), [1]);
        assert_eq!(ok(json!({"term": {"meta.flag": true}})), [1]);
        assert_eq!(ok(json!({"terms": {"meta.color": ["blue", "green"]}})), [2]);
        // Analyzed by the field's tokenizer, so any case of a word matches.
        assert_eq!(ok(json!({"match": {"meta.tags": "BIG"}})), [1]);
        assert_eq!(
            ok(json!({"match": {"meta.tags": {"query": "big small", "operator": "and"}}})),
            Vec::<i64>::new()
        );
        assert_eq!(ok(json!({"match": {"meta.tags": "roof"}})), [1, 2]);
        assert_eq!(ok(json!({"match_phrase": {"meta.tags": "big roof"}})), [1]);
        assert_eq!(
            ok(json!({"match_phrase": {"meta.tags": "roof big"}})),
            Vec::<i64>::new()
        );
        assert_eq!(ok(json!({"range": {"meta.n": {"gte": 2, "lt": 9}}})), [1]);
        assert_eq!(ok(json!({"range": {"meta.n": {"gt": 0}}})), [1, 2, 3]);
        assert_eq!(ok(json!({"exists": {"field": "meta.n"}})), [1, 2, 3]);
        assert_eq!(ok(json!({"exists": {"field": "meta.flag"}})), [1, 3]);
        assert_eq!(ok(json!({"exists": {"field": "meta"}})), [1, 2, 3]);
        assert_eq!(
            ok(json!({"exists": {"field": "meta.nosuch"}})),
            Vec::<i64>::new()
        );
        assert_eq!(
            ok(json!({"multi_match": {"query": "red", "fields": ["meta.color", "body"]}})),
            [1, 3]
        );
        assert_eq!(
            ok(
                json!({"bool": {"must": {"term": {"meta.color": "red"}}, "filter": {"range": {"meta.n": {"lt": 3}}}}})
            ),
            [3]
        );
        // Pattern queries see the strings at their own path only.
        assert_eq!(ok(json!({"prefix": {"meta.color": "re"}})), [1, 3]);
        assert_eq!(ok(json!({"prefix": {"meta.color": "b"}})), [2]); // blue, not "big" of tags
        assert_eq!(ok(json!({"prefix": {"meta.tags": "b"}})), [1]);
        assert_eq!(ok(json!({"wildcard": {"meta.color": "*e*"}})), [1, 2, 3]);
        assert_eq!(ok(json!({"wildcard": {"meta.color": "*ed"}})), [1, 3]);
        assert_eq!(ok(json!({"regexp": {"meta.color": "r.d|blue"}})), [1, 2, 3]);
        assert_eq!(
            ok(json!({"regexp": {"meta.color": "e.*"}})),
            Vec::<i64>::new()
        ); // anchored
        assert_eq!(ok(json!({"prefix": {"meta.n": "5"}})), Vec::<i64>::new()); // a number, not a string
        assert_eq!(
            ok(json!({"prefix": {"meta.nosuch": "r"}})),
            Vec::<i64>::new()
        );
        assert_eq!(
            ok(json!({"prefix": {"meta.color": "RE"}})),
            Vec::<i64>::new()
        );
        assert_eq!(
            ok(json!({"prefix": {"meta.color": {"value": "RE", "case_insensitive": true}}})),
            [1, 3]
        );
        assert_eq!(
            ok(json!({"term": {"meta.color": {"value": "BLUE", "case_insensitive": true}}})),
            [2]
        );
        assert_eq!(
            ok(json!({"fuzzy": {"meta.color": {"value": "rde"}}})),
            [1, 3]
        ); // a transposition
        assert_eq!(
            ok(json!({"match": {"meta.tags": {"query": "rooof", "fuzziness": 1}}})),
            [1, 2]
        );
        assert_eq!(
            ok(
                json!({"match": {"meta.tags": {"query": "BIG roof", "fuzziness": "AUTO", "operator": "and"}}})
            ),
            [1]
        );
        assert!(
            fails(json!({"match_phrase_prefix": {"meta.tags": "big r"}}), "")
                .contains("not a path inside")
        );
        assert!(fails(json!({"prefix": {"meta": "x"}}), "").contains("needs an indexed text field"));
        assert!(fails(json!({"prefix": {"id": "1"}}), "").contains("needs an indexed text field"));
        assert!(fails(json!({"regexp": {"meta.color": "("}}), "").contains("regular expression"));
        assert!(fails(json!({"term": {"meta.color": null}}), "").contains("cannot use"));
        // A path inside something that is not a JSON field is not a field.
        assert!(fails(json!({"term": {"body.x": "a"}}), "").contains("no field"));
    }

    #[test]
    fn leaves_out_missing_fields_on_request() {
        let missing = [
            json!({"term": {"nosuch": "a"}}),
            json!({"match": {"nosuch": "a"}}),
            json!({"terms": {"nosuch": ["a"]}}),
            json!({"range": {"nosuch": {"gt": 1}}}),
            json!({"exists": {"field": "nosuch"}}),
            json!({"multi_match": {"query": "a", "fields": ["nosuch", "nosuch.x"]}}),
            json!({"query_string": {"query": "a", "fields": ["nosuch"]}}),
        ];
        for q in missing {
            assert!(fails(q.clone(), "").contains("no field"), "{q}");
            assert_eq!(
                ids(q.clone(), r#"{"ignore_unmapped": true}"#).unwrap(),
                Vec::<i64>::new(),
                "{q}"
            );
        }
        // Only what is missing matches nothing.
        let q = json!({"bool": {"should": [{"term": {"nosuch": "a"}}, {"term": {"meta.color": "blue"}}]}});
        assert_eq!(ids(q, r#"{"ignore_unmapped": true}"#).unwrap(), [2]);
        let q = json!({"multi_match": {"query": "red", "fields": ["nosuch", "meta.color"]}});
        assert_eq!(ids(q, r#"{"ignore_unmapped": true}"#).unwrap(), [1, 3]);
        let q = json!({"bool": {"must_not": {"term": {"nosuch": "a"}}}});
        assert_eq!(
            ids(q, r#"{"ignore_unmapped": true}"#).unwrap(),
            [1, 2, 3, 4]
        );
        // A type error is still an error.
        assert!(
            fails(json!({"term": {"id": "x"}}), r#"{"ignore_unmapped": true}"#)
                .contains("cannot use")
        );
    }

    #[test]
    fn exists_falls_back_to_the_terms_of_an_indexed_text_field() {
        // `body` and `kw` are indexed text, not fast fields.
        assert_eq!(ok(json!({"exists": {"field": "body"}})), [1, 3]);
        assert_eq!(ok(json!({"exists": {"field": "kw"}})), [1, 2]); // even "a\nb"
        assert_eq!(ok(json!({"exists": {"field": "id"}})), [1, 2, 3, 4]); // fast
        let err = fails(json!({"exists": {"field": "note"}}), "");
        assert!(err.contains("neither"), "{err}");
    }
}
