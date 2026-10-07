//! Properties of the search, over random queries, options and exclusions, on a
//! corpus built once: the answers must not depend on how many threads ran, on
//! how the splits are cut, or on how excluded documents are skipped, and must
//! agree with a naive computation from the full ranking.

use std::collections::{BTreeSet, HashSet};
use std::sync::OnceLock;

use proptest::prelude::*;
use roaring::{RoaringBitmap, RoaringTreemap};
use serde_json::{json, Value};

use super::*;
use crate::split::testing::split;

const SPLITS: usize = 4;
const PER_SPLIT: usize = 150;
const WORDS: [&str; 8] = [
    "roof", "slate", "tile", "wall", "door", "glass", "brick", "steel",
];
const TAGS: usize = 6;

/// Splits of `PER_SPLIT` documents: ids are unique across them, the body a few
/// words, the tag one of a few values (a document in eight has none).
fn corpus() -> &'static [Split] {
    static CORPUS: OnceLock<Vec<Split>> = OnceLock::new();
    CORPUS.get_or_init(|| {
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let mut next = move |n: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % n as u64) as usize
        };
        (0..SPLITS)
            .map(|s| {
                let docs: Vec<String> = (0..PER_SPLIT)
                    .map(|n| {
                        let words: Vec<&str> =
                            (0..1 + next(6)).map(|_| WORDS[next(WORDS.len())]).collect();
                        let id = s * PER_SPLIT + n;
                        match next(8) {
                            0 => format!(r#"{{"id": {id}, "body": "{}"}}"#, words.join(" ")),
                            _ => format!(
                                r#"{{"id": {id}, "body": "{}", "tag": "t{}"}}"#,
                                words.join(" "),
                                next(TAGS)
                            ),
                        }
                    })
                    .collect();
                split(&docs.iter().map(String::as_str).collect::<Vec<_>>())
            })
            .collect()
    })
}

fn refs() -> Vec<&'static Split> {
    corpus().iter().collect()
}

fn word() -> impl Strategy<Value = &'static str> {
    prop::sample::select(&WORDS[..])
}

fn tag() -> impl Strategy<Value = String> {
    (0..TAGS + 1).prop_map(|n| format!("t{n}"))
}

fn id() -> impl Strategy<Value = i64> {
    0..(SPLITS * PER_SPLIT + 20) as i64
}

fn leaf() -> impl Strategy<Value = Value> {
    prop_oneof![
        word().prop_map(|w| json!({"match": {"body": w}})),
        (word(), word()).prop_map(
            |(a, b)| json!({"match": {"body": {"query": format!("{a} {b}"), "operator": "and"}}})
        ),
        (word(), word()).prop_map(|(a, b)| json!({"match_phrase": {"body": format!("{a} {b}")}})),
        tag().prop_map(|t| json!({"term": {"tag": t}})),
        id().prop_map(|i| json!({"term": {"id": i}})),
        word().prop_map(|w| json!({"prefix": {"body": &w[..2]}})),
        tag().prop_map(|t| json!({"prefix": {"tag": &t[..1]}})),
        tag().prop_map(|t| json!({"wildcard": {"tag": format!("{}?", &t[..1])}})),
        (0..TAGS).prop_map(|n| json!({"regexp": {"tag": format!("t[0-{n}]")}})),
        (id(), id()).prop_map(|(a, b)| json!({"range": {"id": {"gte": a.min(b), "lt": a.max(b)}}})),
        Just(json!({"exists": {"field": "tag"}})),
        Just(json!({"match_all": {}})),
        Just(json!({"match_none": {}})),
    ]
}

/// Queries of leaves and what combines them. No phrase is excluded: tantivy
/// 0.26.2's phrase scorer asserts, in debug builds only, when an exclusion asks
/// about a document before the one it has reached (`bool` with a `must_not` of
/// a `match_phrase`, as `dis_max` or a second `must` seeks it). Release builds,
/// which are what ships, answer correctly: `must_not` of a `must_not` of a
/// phrase counts the phrase's matches.
fn dsl() -> impl Strategy<Value = Value> {
    leaf().prop_recursive(3, 12, 3, |inner| {
        let list = || prop::collection::vec(inner.clone(), 0..3);
        let negated = || prop::collection::vec(leaf().prop_filter("no phrase", |q| !q.to_string().contains("match_phrase")), 0..3);
        prop_oneof![
            (list(), list(), negated(), list()).prop_map(|(must, should, must_not, filter)| {
                json!({"bool": {"must": must, "should": should, "must_not": must_not, "filter": filter}})
            }),
            inner.clone().prop_map(|q| json!({"constant_score": {"filter": q}})),
            prop::collection::vec(inner.clone(), 1..3).prop_map(|queries| json!({"dis_max": {"queries": queries, "tie_breaker": 0.3}})),
        ]
    })
}

/// A query: in tantivy's syntax or the DSL.
fn query() -> impl Strategy<Value = String> {
    prop_oneof![
        word().prop_map(str::to_owned),
        (word(), word()).prop_map(|(a, b)| format!("{a} {b}")),
        (word(), word()).prop_map(|(a, b)| format!("+{a} -{b}")),
        (word(), word()).prop_map(|(a, b)| format!("\"{a} {b}\"")),
        tag().prop_map(|t| format!("tag:{t}")),
        (id(), id()).prop_map(|(a, b)| format!("id:[{} TO {}]", a.min(b), a.max(b))),
        dsl().prop_map(|q| q.to_string()),
    ]
}

/// How hits are ordered, cut and cut down: options for `tantivy_search`.
fn hit_options() -> impl Strategy<Value = Value> {
    (
        prop::option::of(1..60usize),
        0..40usize,
        prop_oneof![
            Just(None),
            Just(Some(json!({"collapse": "tag"}))),
            Just(Some(json!({"sort": "id"}))),
            Just(Some(json!({"sort": {"field": "id", "order": "desc"}}))),
            Just(Some(json!({"sort": "tag"}))),
            Just(Some(json!({"sort": {"field": "tag", "order": "desc"}}))),
        ],
        any::<bool>(),
    )
        .prop_map(|(top_k, offset, extra, global)| {
            let mut options =
                json!({"fast": ["id", "tag"], "offset": offset, "global_stats": global});
            if let Some(k) = top_k {
                options["top_k"] = json!(k);
            }
            if let Some(Value::Object(extra)) = extra {
                options.as_object_mut().unwrap().extend(extra);
            }
            options
        })
}

fn request<'a>(
    splits: &'a [&'a Split],
    query: &'a str,
    options: &Value,
    exclude: Option<&'a Exclude>,
    threads: usize,
) -> Request<'a> {
    Request::new(splits, query, &options.to_string(), exclude)
        .unwrap_or_else(|e| panic!("{query} {options}: {e:#}"))
        .with_threads(threads)
}

fn hits_of(request: &Request) -> Vec<Hit> {
    request.hits().unwrap_or_else(|e| panic!("{:#}", e))
}

fn doc(hit: &Hit) -> Value {
    serde_json::from_str(&hit.doc).unwrap()
}

fn id_of(hit: &Hit) -> i64 {
    doc(hit)["id"].as_i64().unwrap()
}

fn tag_of(hit: &Hit) -> Option<String> {
    doc(hit)["tag"].as_str().map(str::to_owned)
}

/// The full ranking of a query: every hit, best first.
fn ranking(splits: &[&Split], query: &str, options: &Value) -> Vec<Hit> {
    let mut options = options.clone();
    let map = options.as_object_mut().unwrap();
    map.remove("top_k");
    map.remove("offset");
    hits_of(&request(splits, query, &options, None, 1))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// The same answer on one thread and on many, for every kind of call.
    #[test]
    fn threads_do_not_change_the_answer(
        query in query(),
        options in hit_options(),
        threads in 2..9usize,
    ) {
        let splits = refs();
        let serial = hits_of(&request(&splits, &query, &options, None, 1));
        let parallel = hits_of(&request(&splits, &query, &options, None, threads));
        prop_assert_eq!(serial, parallel);
        let count = |extra: Value, threads| {
            let mut options = json!({"global_stats": options["global_stats"]});
            options.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            request(&splits, &query, &options, None, threads).count().unwrap()
        };
        for extra in [json!({}), json!({"limit": 7}), json!({"distinct": "tag"}), json!({"distinct": "id"})] {
            prop_assert_eq!(count(extra.clone(), 1), count(extra, threads));
        }
        let aggs = r#"{"tags": {"terms": {"field": "tag", "size": 10}}, "ids": {"max": {"field": "id"}}}"#;
        let aggregate = |threads| {
            request(&splits, &query, &json!({}), None, threads).aggregate(aggs).unwrap()
        };
        prop_assert_eq!(aggregate(1), aggregate(threads));
    }

    /// Cutting the same splits differently (one split of everything, or the
    /// four) finds the same documents, and a page is a slice of the ranking.
    #[test]
    fn pages_are_slices_of_the_ranking(query in query(), options in hit_options()) {
        let splits = refs();
        let full = ranking(&splits, &query, &options);
        let offset = options["offset"].as_u64().unwrap() as usize;
        let size = options.get("top_k").and_then(Value::as_u64).map_or(usize::MAX, |k| k as usize);
        let page = hits_of(&request(&splits, &query, &options, None, 3));
        let want: Vec<&Hit> = full.iter().skip(offset).take(size).collect();
        prop_assert_eq!(page.iter().collect::<Vec<_>>(), want);
    }

    /// Excluded documents are never found, never crowd out a hit, and change
    /// no score: the result is the ranking without them.
    #[test]
    fn exclusion_is_the_ranking_without_the_documents(
        query in query(),
        top_k in prop::option::of(1..60usize),
        dead in prop::collection::btree_set(id(), 0..300),
        threads in 1..5usize,
    ) {
        let splits = refs();
        let exclude = Exclude::from_ids(dead.iter().copied());
        let options = json!({"fast": ["id", "tag"], "exclude_field": "id"});
        let full = ranking(&splits, &query, &options);
        let alive: Vec<&Hit> = full.iter().filter(|h| !dead.contains(&id_of(h))).collect();
        let mut with = options.clone();
        if let Some(k) = top_k {
            with["top_k"] = json!(k);
        }
        let found = hits_of(&request(&splits, &query, &with, Some(&exclude), threads));
        let want: Vec<&Hit> = alive.iter().copied().take(top_k.unwrap_or(usize::MAX)).collect();
        prop_assert_eq!(found.iter().collect::<Vec<_>>(), want);
        // Counts, caps and distinct values see the same documents.
        let count = |extra: Value| {
            let mut options = options.clone();
            options.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            request(&splits, &query, &options, Some(&exclude), threads).count().unwrap()
        };
        prop_assert_eq!(count(json!({})), alive.len() as u64);
        prop_assert_eq!(count(json!({"limit": 5})), (alive.len() as u64).min(6));
        let tags: HashSet<String> = alive.iter().filter_map(|h| tag_of(h)).collect();
        prop_assert_eq!(count(json!({"distinct": "tag"})), tags.len() as u64);
        let ids: HashSet<i64> = alive.iter().map(|h| id_of(h)).collect();
        prop_assert_eq!(count(json!({"distinct": "id"})), ids.len() as u64);
    }

    /// Collapse keeps the first hit of each tag in the ranking, documents
    /// without a tag being one group.
    #[test]
    fn collapse_keeps_the_best_of_each_group(
        query in query(),
        top_k in prop::option::of(1..10usize),
        offset in 0..5usize,
    ) {
        let splits = refs();
        let options = json!({"fast": ["id", "tag"]});
        let full = ranking(&splits, &query, &options);
        let mut seen = HashSet::new();
        let best: Vec<&Hit> = full.iter().filter(|h| seen.insert(tag_of(h))).collect();
        let mut collapsed = json!({"fast": ["id", "tag"], "collapse": "tag", "offset": offset});
        if let Some(k) = top_k {
            collapsed["top_k"] = json!(k);
        }
        let found = hits_of(&request(&splits, &query, &collapsed, None, 2));
        let want: Vec<&Hit> = best.into_iter().skip(offset).take(top_k.unwrap_or(usize::MAX)).collect();
        prop_assert_eq!(found.iter().collect::<Vec<_>>(), want);
    }

    /// A query that is not a query fails or finds something; it never panics.
    #[test]
    fn arbitrary_queries_do_not_panic(value in arbitrary_json(), as_text in any::<bool>()) {
        let splits = refs();
        let text = if as_text { value.to_string() } else { json!({"bool": {"must": value}}).to_string() };
        if let Ok(request) = Request::new(&splits, &text, "{}", None) {
            let _ = request.count();
            let _ = request.hits();
        }
    }

    /// Whatever bytes arrive as an exclusion bitmap, they are read or refused.
    #[test]
    fn arbitrary_bytes_are_not_a_bitmap_that_panics(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        let _ = Exclude::from_roaring(&bytes);
    }

    /// Both portable formats read back what was written.
    #[test]
    fn bitmaps_round_trip(
        small in prop::collection::btree_set(any::<u32>(), 0..50),
        big in prop::collection::btree_set(any::<u64>(), 0..50),
    ) {
        let bitmap: RoaringBitmap = small.iter().copied().collect();
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).unwrap();
        let read = Exclude::from_roaring(&bytes).unwrap();
        prop_assert_eq!(read.set.iter().collect::<BTreeSet<u64>>(), small.iter().map(|&i| u64::from(i)).collect::<BTreeSet<u64>>());
        let treemap: RoaringTreemap = big.iter().copied().collect();
        let mut bytes = Vec::new();
        treemap.serialize_into(&mut bytes).unwrap();
        let read = Exclude::from_roaring(&bytes).unwrap();
        prop_assert_eq!(read.set.iter().collect::<BTreeSet<u64>>(), big);
    }
}

/// JSON with the shape of a query: keys the DSL knows and keys it does not.
fn arbitrary_json() -> impl Strategy<Value = Value> {
    let key = prop_oneof![
        prop::sample::select(vec![
            "bool",
            "must",
            "should",
            "must_not",
            "filter",
            "match",
            "match_phrase",
            "match_phrase_prefix",
            "multi_match",
            "term",
            "terms",
            "prefix",
            "wildcard",
            "regexp",
            "fuzzy",
            "exists",
            "range",
            "constant_score",
            "dis_max",
            "queries",
            "more_like_this",
            "query_string",
            "simple_query_string",
            "query",
            "fields",
            "value",
            "boost",
            "gte",
            "lt",
            "like",
            "operator",
            "fuzziness",
            "case_insensitive",
            "minimum_should_match",
            "tie_breaker",
            "slop",
            "body",
            "tag",
            "id",
            "meta.x",
            "nosuch",
        ])
        .prop_map(str::to_owned),
        "[a-z._^]{0,6}",
    ];
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        any::<f64>().prop_filter_map("finite", |f| serde_json::Number::from_f64(f)
            .map(Value::Number)),
        "\\PC{0,8}".prop_map(Value::from),
        word().prop_map(Value::from),
    ];
    leaf.prop_recursive(5, 40, 4, move |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::vec((key.clone(), inner), 0..4)
                .prop_map(|pairs| Value::Object(pairs.into_iter().collect())),
        ]
    })
}

/// Found by the properties above: a group whose best hit came from a later
/// split was placed where its first hit had been seen, so equal scores came
/// out in an order that depended on the page.
#[test]
fn collapsed_ties_keep_the_order_of_the_ranking() {
    let splits = refs();
    let options = json!({"fast": ["id", "tag"]});
    let full = ranking(&splits, "steel", &options);
    let mut seen = HashSet::new();
    let best: Vec<&Hit> = full.iter().filter(|h| seen.insert(tag_of(h))).collect();
    let collapsed = hits_of(&request(
        &splits,
        "steel",
        &json!({"fast": ["id", "tag"], "collapse": "tag"}),
        None,
        1,
    ));
    assert_eq!(collapsed.iter().collect::<Vec<_>>(), best);
    assert!(
        collapsed.windows(2).any(|w| w[0].score == w[1].score),
        "the corpus has ties to order"
    );
}
