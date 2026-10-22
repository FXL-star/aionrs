use std::collections::HashMap;

use reqwest::header::{HeaderMap, HeaderValue};

use super::*;

#[test]
fn applies_provider_headers_to_an_empty_map() {
    let mut headers = HeaderMap::new();
    apply_dynamic_headers(
        &mut headers,
        Some(HashMap::from([("Authorization".to_owned(), "Bearer fresh".to_owned())])),
    );

    assert_eq!(
        headers.get("authorization").and_then(|value| value.to_str().ok()),
        Some("Bearer fresh")
    );
}

#[test]
fn keeps_statically_configured_headers() {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Token user-owned"));
    apply_dynamic_headers(
        &mut headers,
        Some(HashMap::from([
            ("Authorization".to_owned(), "Bearer fresh".to_owned()),
            ("X-Trace".to_owned(), "1".to_owned()),
        ])),
    );

    assert_eq!(
        headers.get("authorization").and_then(|value| value.to_str().ok()),
        Some("Token user-owned")
    );
    assert_eq!(headers.get("x-trace").and_then(|value| value.to_str().ok()), Some("1"));
}

#[test]
fn ignores_malformed_provider_entries() {
    let mut headers = HeaderMap::new();
    apply_dynamic_headers(
        &mut headers,
        Some(HashMap::from([
            ("bad header".to_owned(), "value".to_owned()),
            ("authorization".to_owned(), "Bearer line\nbreak".to_owned()),
        ])),
    );

    assert!(headers.is_empty());
}

#[test]
fn missing_provider_headers_are_a_noop() {
    let mut headers = HeaderMap::new();
    headers.insert("accept", HeaderValue::from_static("application/json"));
    let before = headers.clone();

    apply_dynamic_headers(&mut headers, None);

    assert_eq!(headers, before);
}
