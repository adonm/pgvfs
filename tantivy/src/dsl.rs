//! OpenSearch query DSL, a subset, compiled to tantivy queries, so callers
//! that speak Elasticsearch/OpenSearch (as Quickwit does) need no query-string
//! step. A query is `{"<type>": body}`, or a request body `{"query": ...}`.
//!
//! Supported: match_all, match_none, bool (must, should, must_not, filter,
//! minimum_should_match), constant_score, dis_max, term, terms, match
//! (operator, minimum_should_match, fuzziness), match_phrase (slop),
//! match_phrase_prefix, multi_match (best_fields, most_fields, phrase,
//! phrase_prefix), prefix, wildcard, regexp, fuzzy, exists, range,
//! query_string and simple_query_string; `boost` where Elasticsearch takes it.
//! Filters (`bool.filter`, `constant_score`) do not score.

use std::ops::Bound;

use anyhow::{anyhow, bail, Context as _, Result};
use serde_json::Value;
use tantivy::query::{
    AllQuery, BooleanQuery, BoostQuery, ConstScoreQuery, DisjunctionMaxQuery, EmptyQuery,
    ExistsQuery, FuzzyTermQuery, Occur, PhrasePrefixQuery, PhraseQuery, Query, QueryParser,
    RangeQuery, RegexQuery, TermQuery, TermSetQuery,
};
use tantivy::schema::{Facet, Field, FieldType, IndexRecordOption, Schema};
use tantivy::{DateTime, Index, Term};

pub fn compile(index: &Index, dsl: &Value) -> Result<Box<dyn Query>> {
    let dsl = match dsl.as_object() {
        Some(o) if o.len() == 1 && o.contains_key("query") => &o["query"],
        _ => dsl,
    };
    Compiler {
        index,
        schema: index.schema(),
    }
    .query(dsl)
}

struct Compiler<'a> {
    index: &'a Index,
    schema: Schema,
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
            "fuzzy_transpositions" | "transpositions" => value.is_boolean(),
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

/// A wildcard pattern (`*`, `?`) as a regular expression.
fn wildcard_regex(pattern: &str) -> Result<String> {
    let mut out = String::new();
    let literal = |out: &mut String, c| {
        if "\\.+*?()|[]{}^$".contains(c) {
            out.push('\\');
        }
        out.push(c);
    };
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            '\\' => literal(
                &mut out,
                chars.next().context("wildcard has a trailing escape")?,
            ),
            c => literal(&mut out, c),
        }
    }
    Ok(out)
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
            "exists" => Ok(boosted(exists(&self.schema, body)?, body)),
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
                let (field, params) = self.field_body(kind, body)?;
                let allowed: &[&str] = match kind.as_str() {
                    "term" | "prefix" | "wildcard" | "regexp" => &["value"],
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
                if matches!(kind.as_str(), "prefix" | "wildcard" | "regexp" | "fuzzy") {
                    let entry = self.schema.get_field_entry(field);
                    anyhow::ensure!(
                        entry.is_indexed() && matches!(entry.field_type(), FieldType::Str(_)),
                        "{kind} needs an indexed text field"
                    );
                }
                let q = match kind.as_str() {
                    "term" => Box::new(TermQuery::new(
                        self.term(field, short(params, "value"))?,
                        IndexRecordOption::WithFreqs,
                    )) as Box<dyn Query>,
                    "terms" => {
                        let values = params.as_array().context("terms takes a list of values")?;
                        let terms = values
                            .iter()
                            .map(|v| self.term(field, v))
                            .collect::<Result<Vec<_>>>()?;
                        Box::new(TermSetQuery::new(terms))
                    }
                    "match" => self.match_query(field, params)?,
                    "match_phrase" => self.phrase(field, params, false)?,
                    "match_phrase_prefix" => self.phrase(field, params, true)?,
                    "prefix" => {
                        let term = self.term(field, short(params, "value"))?;
                        Box::new(FuzzyTermQuery::new_prefix(term, 0, false))
                    }
                    "wildcard" => {
                        let pattern = text(short(params, "value"))?;
                        Box::new(RegexQuery::from_pattern(&wildcard_regex(&pattern)?, field)?)
                    }
                    "regexp" => Box::new(RegexQuery::from_pattern(
                        &text(short(params, "value"))?,
                        field,
                    )?),
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
                            self.term(field, &Value::from(value))?,
                            d,
                            transpositions,
                        ))
                    }
                    _ => self.range(field, params)?,
                };
                Ok(if kind == "terms" {
                    parameters(body, kind, &[self.schema.get_field_entry(field).name()])?;
                    boosted(q, body)
                } else {
                    boosted(q, params)
                })
            }
            other => bail!("unsupported query type {other:?}"),
        }
    }

    /// `{"<field>": params}` of a field-level query.
    fn field_body<'v>(&self, kind: &str, body: &'v Value) -> Result<(Field, &'v Value)> {
        let obj = body
            .as_object()
            .with_context(|| format!("{kind} takes {{\"<field>\": ...}}"))?;
        let mut fields = obj
            .iter()
            .filter(|(k, _)| kind != "terms" || (k.as_str() != "boost" && k.as_str() != "_name"));
        let (Some((name, params)), None) = (fields.next(), fields.next()) else {
            bail!("{kind} takes one field");
        };
        Ok((self.field(name)?, params))
    }

    fn field(&self, name: &str) -> Result<Field> {
        self.schema
            .get_field(name)
            .map_err(|_| anyhow!("no field {name:?}"))
    }

    /// A field value as a term: text verbatim (not analyzed), numbers, dates
    /// (RFC 3339 or epoch milliseconds), booleans, IP addresses, facets.
    fn term(&self, field: Field, v: &Value) -> Result<Term> {
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

    /// The field's analyzer applied to `text`: (position, term) per token.
    /// Fields that are not analyzed text give the value itself.
    fn tokens(&self, field: Field, v: &Value) -> Result<Vec<(usize, Term)>> {
        if !matches!(
            self.schema.get_field_entry(field).field_type(),
            FieldType::Str(_)
        ) {
            return Ok(vec![(0, self.term(field, v)?)]);
        }
        let mut analyzer = self.index.tokenizer_for_field(field)?;
        let text = text(v)?;
        let mut stream = analyzer.token_stream(&text);
        let mut out = Vec::new();
        while stream.advance() {
            let token = stream.token();
            out.push((token.position, Term::from_field_text(field, &token.text)));
        }
        Ok(out)
    }

    fn match_query(&self, field: Field, params: &Value) -> Result<Box<dyn Query>> {
        let tokens = self.tokens(field, short(params, "query"))?;
        let fuzziness = params.get("fuzziness");
        let transpositions = params
            .get("fuzzy_transpositions")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let clauses = tokens
            .into_iter()
            .map(|(_, term)| {
                Ok(match fuzziness {
                    Some(f) => {
                        let word = term.value().as_str().unwrap_or_default().to_owned();
                        Box::new(FuzzyTermQuery::new(
                            term,
                            distance(f, &word)?,
                            transpositions,
                        )) as Box<dyn Query>
                    }
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

    fn phrase(&self, field: Field, params: &Value, prefix: bool) -> Result<Box<dyn Query>> {
        let tokens = self.tokens(field, short(params, "query"))?;
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

    fn range(&self, field: Field, params: &Value) -> Result<Box<dyn Query>> {
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
                (Some(v), _) => Bound::Included(self.term(field, v)?),
                (None, Some(v)) => Bound::Excluded(self.term(field, v)?),
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
        let fields: Vec<(Field, f32)> = match body.get("fields") {
            Some(Value::Array(names)) => names
                .iter()
                .map(|n| {
                    let n = n.as_str().context("multi_match fields are names")?;
                    let (name, boost) = match n.split_once('^') {
                        Some((name, b)) => (name, b.parse::<f32>().context("field boost")?),
                        None => (n, 1.0),
                    };
                    anyhow::ensure!(boost.is_finite() && boost >= 0.0, "invalid field boost");
                    Ok((self.field(name)?, boost))
                })
                .collect::<Result<_>>()?,
            None => crate::search::text_fields(&self.schema)
                .into_iter()
                .map(|f| (f, 1.0))
                .collect(),
            Some(other) => bail!("multi_match fields: {other}"),
        };
        let kind = body
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("best_fields");
        let per_field = fields
            .into_iter()
            .map(|(field, boost)| {
                let q = match kind {
                    "best_fields" | "most_fields" => self.match_query(field, body)?,
                    "phrase" => self.phrase(field, body, false)?,
                    "phrase_prefix" => self.phrase(field, body, true)?,
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
        let fields = match (body.get("fields"), body.get("default_field")) {
            (Some(Value::Array(names)), _) => names
                .iter()
                .map(|n| self.field(n.as_str().context("field names")?))
                .collect::<Result<_>>()?,
            (_, Some(Value::String(name))) => vec![self.field(name)?],
            _ => crate::search::text_fields(&self.schema),
        };
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

/// `{"exists": {"field": "<name>"}}`: needs a fast field.
fn exists(schema: &Schema, body: &Value) -> Result<Box<dyn Query>> {
    parameters(body, "exists", &["field"])?;
    let name = body
        .get("field")
        .and_then(Value::as_str)
        .context("exists takes {\"field\": \"<name>\"}")?;
    schema
        .get_field(name)
        .map_err(|_| anyhow!("no field {name:?}"))?;
    Ok(Box::new(ExistsQuery::new(name.to_owned(), false)))
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
                .map(|(_, _, d)| {
                    serde_json::from_str::<serde_json::Value>(&d).unwrap()["id"]
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
                .map(|h| h.1)
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
    fn rejects_what_it_does_not_support() {
        assert!(fails(json!({"nested": {"path": "x"}})).contains("unsupported query type"));
        assert!(fails(json!({"match": {"nosuch": "x"}})).contains("no field"));
        assert!(fails(json!({"term": {"id": "x"}})).contains("cannot use"));
        assert!(fails(json!({"match": {"body": "x"}, "term": {"id": 1}})).contains("one key"));
        assert!(fails(json!({"exists": {"field": "body"}})).contains("fast"));
        assert!(!fails(json!({"query_string": {"query": "body:("}})).is_empty());
        for q in [
            json!({"bool": {"filters": {"term": {"id": 1}}}}),
            json!({"bool": null}),
            json!({"match_all": {"filter": {"term": {"id": 1}}}}),
            json!({"match": {"body": {"query": "fox", "operator": "xor"}}}),
            json!({"match": {"body": {"query": "fox", "fuzziness": 3}}}),
            json!({"prefix": {"tag": {"value": "a", "case_insensitive": true}}}),
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
