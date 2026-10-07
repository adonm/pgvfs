//! Regular-expression queries over a text field's terms, or over the strings at
//! a path inside a JSON field. Tantivy's `RegexQuery` takes a whole field, and
//! the terms of a JSON field's paths share one dictionary, each starting with
//! its path: the automaton here skips that prefix, then runs the expression.
//!
//! Adapted from `quickwit-query` (https://github.com/quickwit-oss/quickwit,
//! `query_ast/regex_query.rs`): `JsonPathPrefix` and `AutomatonQuery`.
//! Apache License 2.0, Copyright 2021-Present Datadog, Inc.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use tantivy::query::{AutomatonWeight, EnableScoring, Query, Weight};
use tantivy::schema::{Field, JsonObjectOptions};
use tantivy::Term;
use tantivy_fst::{Automaton, Regex};

/// The bytes that start, in a JSON field's term dictionary, every string term
/// under `path`: the path, its end marker and the string type code.
fn json_str_prefix(field: Field, path: &str, options: &JsonObjectOptions) -> Vec<u8> {
    let mut term = Term::from_field_json_path(field, path, options.is_expand_dots_enabled());
    term.append_type_and_str("");
    // The first byte marks the term as JSON; the dictionary does not hold it.
    term.value().as_serialized()[1..].to_vec()
}

/// A regular expression that must follow `prefix`.
struct PathPrefix {
    prefix: Vec<u8>,
    regex: Regex,
}

#[derive(Clone, Debug, PartialEq)]
enum State<S> {
    Prefix(usize),
    Inner(S),
    Failed,
}

impl Automaton for PathPrefix {
    type State = State<<Regex as Automaton>::State>;

    fn start(&self) -> Self::State {
        if self.prefix.is_empty() {
            State::Inner(self.regex.start())
        } else {
            State::Prefix(0)
        }
    }

    fn is_match(&self, state: &Self::State) -> bool {
        match state {
            State::Inner(inner) => self.regex.is_match(inner),
            State::Prefix(_) | State::Failed => false,
        }
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        match state {
            State::Prefix(at) => {
                if self.prefix.get(*at) != Some(&byte) {
                    return State::Failed;
                }
                if at + 1 == self.prefix.len() {
                    State::Inner(self.regex.start())
                } else {
                    State::Prefix(at + 1)
                }
            }
            State::Inner(inner) => State::Inner(self.regex.accept(inner, byte)),
            State::Failed => State::Failed,
        }
    }

    // The dictionary search prunes whole subtrees on this.
    fn can_match(&self, state: &Self::State) -> bool {
        match state {
            State::Prefix(_) => true,
            State::Inner(inner) => self.regex.can_match(inner),
            State::Failed => false,
        }
    }

    fn will_always_match(&self, state: &Self::State) -> bool {
        match state {
            State::Inner(inner) => self.regex.will_always_match(inner),
            State::Prefix(_) | State::Failed => false,
        }
    }
}

/// Documents with a term of `field` that matches, as the terms of a regular
/// expression. Constant score.
#[derive(Clone)]
pub struct PatternQuery {
    field: Field,
    automaton: Arc<PathPrefix>,
}

impl std::fmt::Debug for PatternQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("PatternQuery")
            .field("field", &self.field)
            .finish_non_exhaustive()
    }
}

impl PatternQuery {
    /// `regex` (tantivy's regex syntax, matching a whole term) over the terms
    /// of `field`, or of the strings at `path` of a JSON field.
    pub fn new(
        field: Field,
        json: Option<(&str, &JsonObjectOptions)>,
        regex: &str,
        case_insensitive: bool,
    ) -> Result<PatternQuery> {
        let regex = if case_insensitive {
            format!("(?i){regex}")
        } else {
            regex.to_owned()
        };
        let regex = Regex::new(&regex).map_err(|e| anyhow!("invalid regular expression: {e}"))?;
        let prefix = match json {
            Some((path, options)) => json_str_prefix(field, path, options),
            None => Vec::new(),
        };
        Ok(PatternQuery {
            field,
            automaton: Arc::new(PathPrefix { prefix, regex }),
        })
    }
}

impl Query for PatternQuery {
    fn weight(&self, _: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(AutomatonWeight::<PathPrefix>::new(
            self.field,
            self.automaton.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_the_path_then_runs_the_expression() {
        let automaton = PathPrefix {
            prefix: b"ab".to_vec(),
            regex: Regex::new("e(f|g.*)").unwrap(),
        };
        let run = |bytes: &[u8]| {
            bytes
                .iter()
                .fold(automaton.start(), |state, &b| automaton.accept(&state, b))
        };
        assert!(automaton.is_match(&run(b"abef")) && automaton.is_match(&run(b"abegh")));
        // A different path, or a prefix of it, or a bad continuation, cannot match.
        for dead in [&b"ac"[..], b"a", b"xbef", b"abx"] {
            let state = run(dead);
            assert!(!automaton.is_match(&state), "{dead:?}");
        }
        assert!(!automaton.can_match(&run(b"ac")) && !automaton.can_match(&run(b"abx")));
        assert!(automaton.can_match(&run(b"a")) && automaton.can_match(&run(b"abe")));
        // No path: the expression alone.
        let plain = PathPrefix {
            prefix: Vec::new(),
            regex: Regex::new("e(f|g.*)").unwrap(),
        };
        assert_eq!(plain.start(), State::Inner(plain.regex.start()));
        assert!(PatternQuery::new(Field::from_field_id(0), None, "(", false).is_err());
    }
}
