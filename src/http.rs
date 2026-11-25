//! A deliberately read-only HTTP client.
//!
//! citop must never change anything on a CI server. This client therefore
//! only exposes GET requests, plus GraphQL queries, which are checked to not
//! contain mutations before they are sent. There is no way to issue any other
//! kind of request through it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use reqwest::header::{self, HeaderMap, HeaderValue};
use reqwest::{Method, Response, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use url::Url;

use crate::secret::Secret;

const USER_AGENT: &str = concat!("citop/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("{method} {url}: HTTP {status}{detail}")]
    Status {
        method: Method,
        url: Box<Url>,
        status: StatusCode,
        detail: String,
    },
    #[error("{method} {url}: request failed")]
    Transport {
        method: Method,
        url: Box<Url>,
        #[source]
        source: reqwest::Error,
    },
    #[error("{url}: could not decode response")]
    Decode {
        url: Box<Url>,
        #[source]
        source: serde_json::Error,
    },
    #[error("refusing to follow pagination link to another origin: {0}")]
    ForeignLink(Box<Url>),
    #[error("refusing to send GraphQL document: {0}")]
    NotReadOnly(String),
    #[error("{url}: GraphQL error: {messages}")]
    GraphQl { url: Box<Url>, messages: String },
}

impl HttpError {
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            HttpError::Status { status, .. } => Some(*status),
            _ => None,
        }
    }

    pub fn is_not_found(&self) -> bool {
        self.status() == Some(StatusCode::NOT_FOUND)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimit {
    pub limit: u64,
    pub remaining: u64,
    pub reset: Option<DateTime<Utc>>,
}

/// A response kept for conditional requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedResponse {
    pub etag: String,
    pub body: Vec<u8>,
    /// The `rel="next"` pagination link of the response.
    pub next: Option<String>,
}

/// Storage for responses to conditional requests. On GitHub, a 304 response
/// does not count against the rate limit. Implementations are best effort.
pub trait ResponseCache: Send + Sync {
    fn load<'a>(&'a self, url: &'a Url) -> BoxFuture<'a, Option<CachedResponse>>;
    fn store<'a>(&'a self, url: &'a Url, response: CachedResponse) -> BoxFuture<'a, ()>;
}

/// A cache that never has anything.
pub struct NoCache;

impl ResponseCache for NoCache {
    fn load<'a>(&'a self, _url: &'a Url) -> BoxFuture<'a, Option<CachedResponse>> {
        Box::pin(async { None })
    }

    fn store<'a>(&'a self, _url: &'a Url, _response: CachedResponse) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}

struct Fetched {
    body: Vec<u8>,
    next: Option<Url>,
}

pub struct ReadOnlyClient {
    http: reqwest::Client,
    cache: Arc<dyn ResponseCache>,
    rate_limit: Mutex<Option<RateLimit>>,
}

impl ReadOnlyClient {
    /// The token is sent as `Authorization: Bearer`, which reqwest strips when
    /// following a redirect to another host.
    pub fn new(
        token: &Secret,
        mut headers: HeaderMap,
        cache: Arc<dyn ResponseCache>,
    ) -> anyhow::Result<Self> {
        let mut auth = HeaderValue::from_str(&format!("Bearer {}", token.expose()))
            .ok()
            .context("token contains characters that are not allowed in an HTTP header")?;
        auth.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, auth);
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .default_headers(headers)
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()
            .context("could not create HTTP client")?;
        Ok(Self {
            http,
            cache,
            rate_limit: Mutex::default(),
        })
    }

    /// The most recently reported API rate limit, if the server sends one.
    pub fn rate_limit(&self) -> Option<RateLimit> {
        *self.rate_limit.lock().unwrap()
    }

    pub async fn get_json<T: DeserializeOwned>(&self, url: Url) -> Result<T, HttpError> {
        let fetched = self.get(&url, false).await?;
        decode(&url, &fetched.body)
    }

    /// Like [`get_json`](Self::get_json), but uses a conditional request if
    /// this URL was fetched before. Only use this for URLs that are requested
    /// repeatedly with the same query, as every distinct URL is cached.
    pub async fn get_json_cached<T: DeserializeOwned>(&self, url: Url) -> Result<T, HttpError> {
        let fetched = self.get(&url, true).await?;
        decode(&url, &fetched.body)
    }

    /// Fetches a JSON array, following `Link: <...>; rel="next"` headers for
    /// up to `max_pages` pages.
    pub async fn get_all<T: DeserializeOwned>(
        &self,
        url: Url,
        max_pages: usize,
    ) -> Result<Vec<T>, HttpError> {
        let mut items = Vec::new();
        let mut next = Some(url);
        for _ in 0..max_pages {
            let Some(url) = next.take() else { break };
            let fetched = self.get(&url, false).await?;
            items.extend(decode::<Vec<T>>(&url, &fetched.body)?);
            if let Some(link) = fetched.next {
                if link.origin() != url.origin() {
                    return Err(HttpError::ForeignLink(Box::new(link)));
                }
                next = Some(link);
            }
        }
        Ok(items)
    }

    #[allow(dead_code)] // used for job logs
    pub async fn get_text(&self, url: Url) -> Result<String, HttpError> {
        let fetched = self.get(&url, false).await?;
        Ok(String::from_utf8_lossy(&fetched.body).into_owned())
    }

    /// Sends a GraphQL query. Documents containing a `mutation` or
    /// `subscription` operation are rejected without being sent.
    #[allow(dead_code)] // used by the GitHub provider
    pub async fn graphql<T: DeserializeOwned>(
        &self,
        endpoint: &Url,
        query: &str,
        variables: serde_json::Value,
    ) -> Result<T, HttpError> {
        #[derive(Deserialize)]
        struct GraphQlResponse<D> {
            data: Option<D>,
            #[serde(default)]
            errors: Vec<GraphQlError>,
        }
        #[derive(Deserialize)]
        struct GraphQlError {
            message: String,
        }

        ensure_read_only(query).map_err(HttpError::NotReadOnly)?;
        let body = serde_json::json!({ "query": query, "variables": variables });
        let response = self
            .http
            .post(endpoint.clone())
            .json(&body)
            .send()
            .await
            .map_err(|source| HttpError::Transport {
                method: Method::POST,
                url: Box::new(endpoint.clone()),
                source,
            })?;
        let body = self.read_body(Method::POST, endpoint, response).await?;
        let response: GraphQlResponse<T> = decode(endpoint, &body)?;
        // Partial data (e.g. one inaccessible repository) is still useful.
        response.data.ok_or_else(|| HttpError::GraphQl {
            url: Box::new(endpoint.clone()),
            messages: response
                .errors
                .into_iter()
                .map(|e| e.message)
                .collect::<Vec<_>>()
                .join("; "),
        })
    }

    async fn get(&self, url: &Url, use_cache: bool) -> Result<Fetched, HttpError> {
        let cached = if use_cache {
            self.cache.load(url).await
        } else {
            None
        };
        let etag = cached
            .as_ref()
            .and_then(|c| HeaderValue::from_str(&c.etag).ok());
        let mut response = self.send_get(url, etag).await?;
        if response.status() == StatusCode::NOT_MODIFIED {
            match cached {
                Some(cached) => {
                    return Ok(Fetched {
                        body: cached.body,
                        next: cached.next.and_then(|n| Url::parse(&n).ok()),
                    });
                }
                // Should not happen, as we only send `If-None-Match` with a cached body.
                None => response = self.send_get(url, None).await?,
            }
        }

        let next = next_link(response.headers());
        let etag = response
            .headers()
            .get(header::ETAG)
            .and_then(|e| e.to_str().ok())
            .map(str::to_string);
        let body = self.read_body(Method::GET, url, response).await?;

        if use_cache && let Some(etag) = etag {
            let response = CachedResponse {
                etag,
                body: body.clone(),
                next: next.as_ref().map(Url::to_string),
            };
            self.cache.store(url, response).await;
        }
        Ok(Fetched { body, next })
    }

    async fn send_get(&self, url: &Url, etag: Option<HeaderValue>) -> Result<Response, HttpError> {
        let mut request = self.http.get(url.clone());
        if let Some(etag) = etag {
            request = request.header(header::IF_NONE_MATCH, etag);
        }
        let response = request
            .send()
            .await
            .map_err(|source| HttpError::Transport {
                method: Method::GET,
                url: Box::new(url.clone()),
                source,
            })?;
        self.record_rate_limit(response.headers());
        Ok(response)
    }

    /// Reads the body of a response, turning non-2xx responses into errors.
    async fn read_body(
        &self,
        method: Method,
        url: &Url,
        response: Response,
    ) -> Result<Vec<u8>, HttpError> {
        let status = response.status();
        let body = response.bytes().await;
        if !status.is_success() {
            let detail = body.ok().map(|b| error_detail(&b)).unwrap_or_default();
            return Err(HttpError::Status {
                method,
                url: Box::new(url.clone()),
                status,
                detail,
            });
        }
        body.map(|b| b.to_vec())
            .map_err(|source| HttpError::Transport {
                method,
                url: Box::new(url.clone()),
                source,
            })
    }

    fn record_rate_limit(&self, headers: &HeaderMap) {
        let get = |names: &[&str]| {
            names
                .iter()
                .find_map(|n| headers.get(*n)?.to_str().ok()?.trim().parse::<u64>().ok())
        };
        // GitHub uses `x-ratelimit-*`, GitLab `ratelimit-*`.
        let limit = get(&["x-ratelimit-limit", "ratelimit-limit"]);
        let remaining = get(&["x-ratelimit-remaining", "ratelimit-remaining"]);
        let reset = get(&["x-ratelimit-reset", "ratelimit-reset"])
            .and_then(|secs| DateTime::from_timestamp(i64::try_from(secs).ok()?, 0));
        if let (Some(limit), Some(remaining)) = (limit, remaining) {
            *self.rate_limit.lock().unwrap() = Some(RateLimit {
                limit,
                remaining,
                reset,
            });
        }
    }
}

fn decode<T: DeserializeOwned>(url: &Url, body: &[u8]) -> Result<T, HttpError> {
    serde_json::from_slice(body).map_err(|source| HttpError::Decode {
        url: Box::new(url.clone()),
        source,
    })
}

/// Extracts a short human-readable message from an error response body.
fn error_detail(body: &[u8]) -> String {
    let message = match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(json) => ["message", "error_description", "error"]
            .iter()
            .find_map(|key| json.get(key))
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| v.to_string())
            }),
        Err(_) => {
            let text = String::from_utf8_lossy(body);
            let text = text.trim();
            (!text.is_empty() && !text.starts_with('<')).then(|| text.to_string())
        }
    };
    match message {
        Some(m) => {
            let m: String = m.chars().take(200).collect();
            format!(": {m}")
        }
        None => String::new(),
    }
}

/// Finds the `rel="next"` URL in `Link` headers (RFC 8288).
fn next_link(headers: &HeaderMap) -> Option<Url> {
    for value in headers.get_all(header::LINK) {
        let Ok(value) = value.to_str() else { continue };
        let mut rest = value;
        while let Some(start) = rest.find('<') {
            let after = &rest[start + 1..];
            let end = after.find('>')?;
            let target = &after[..end];
            let params_end = after[end..].find('<').map_or(after.len(), |i| end + i);
            // Drop the `,` that separates this link from the next one.
            let params = after[end + 1..params_end].trim_end().trim_end_matches(',');
            let is_next = params.split(';').any(|param| {
                let Some((key, val)) = param.split_once('=') else {
                    return false;
                };
                key.trim().eq_ignore_ascii_case("rel")
                    && val
                        .trim()
                        .trim_matches('"')
                        .split_ascii_whitespace()
                        .any(|rel| rel.eq_ignore_ascii_case("next"))
            });
            if is_next {
                return Url::parse(target).ok();
            }
            rest = &after[params_end..];
        }
    }
    None
}

/// Checks that a GraphQL document contains only queries (and fragments), by
/// looking at the keywords outside of any selection set. Strings and comments
/// are skipped so they cannot hide or fake a keyword. Errs on the side of
/// rejecting documents.
pub(crate) fn ensure_read_only(doc: &str) -> Result<(), String> {
    fn is_name_start(c: u8) -> bool {
        c.is_ascii_alphabetic() || c == b'_'
    }
    fn is_name(c: u8) -> bool {
        c.is_ascii_alphanumeric() || c == b'_'
    }

    let b = doc.as_bytes();
    let mut i = 0;
    let mut depth = 0usize;
    let mut operations = 0usize;
    while i < b.len() {
        match b[i] {
            b'#' => {
                while i < b.len() && b[i] != b'\n' && b[i] != b'\r' {
                    i += 1;
                }
            }
            b'"' if b[i..].starts_with(b"\"\"\"") => {
                i += 3;
                loop {
                    if i >= b.len() {
                        return Err("unterminated block string".into());
                    }
                    if b[i..].starts_with(b"\\\"\"\"") {
                        i += 4;
                    } else if b[i..].starts_with(b"\"\"\"") {
                        i += 3;
                        break;
                    } else {
                        i += 1;
                    }
                }
            }
            b'"' => {
                i += 1;
                loop {
                    match b.get(i) {
                        None | Some(b'\n') | Some(b'\r') => {
                            return Err("unterminated string".into());
                        }
                        Some(b'\\') => i += 2,
                        Some(b'"') => {
                            i += 1;
                            break;
                        }
                        Some(_) => i += 1,
                    }
                }
            }
            b'{' => {
                if depth == 0 {
                    operations += 1;
                }
                depth += 1;
                i += 1;
            }
            b'}' => {
                depth = depth.checked_sub(1).ok_or("unbalanced braces")?;
                i += 1;
            }
            // Variable and directive names are not keywords.
            b'$' | b'@' => {
                i += 1;
                while i < b.len() && is_name(b[i]) {
                    i += 1;
                }
            }
            c if is_name_start(c) => {
                let start = i;
                while i < b.len() && is_name(b[i]) {
                    i += 1;
                }
                let word = &doc[start..i];
                if depth == 0 && (word == "mutation" || word == "subscription") {
                    return Err(format!("`{word}` operations are not allowed"));
                }
            }
            _ => i += 1,
        }
    }
    if depth != 0 {
        return Err("unbalanced braces".into());
    }
    if operations == 0 {
        return Err("document contains no operation".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderName;

    #[test]
    fn allows_queries() {
        assert!(ensure_read_only("{ viewer { login } }").is_ok());
        assert!(
            ensure_read_only("query Q($mutation: Int = 3) @mutation { a(x: \"mutation\") { b } }")
                .is_ok()
        );
        assert!(ensure_read_only("# mutation\nquery { mutation { x } }").is_ok());
        assert!(ensure_read_only("query { ...F } fragment F on Q { a }").is_ok());
        assert!(ensure_read_only("query { a(s: \"\"\"mutation \\\"\"\" }\"\"\") }").is_ok());
        assert!(ensure_read_only("query { a(s: \"ünïcödé\") }").is_ok());
    }

    #[test]
    fn rejects_mutations_and_junk() {
        assert!(ensure_read_only("mutation { deleteRepo(id: 1) }").is_err());
        assert!(ensure_read_only("  mutation{x}").is_err());
        assert!(ensure_read_only("query { a } mutation M { b }").is_err());
        assert!(ensure_read_only("subscription { a }").is_err());
        assert!(ensure_read_only("query { a(s: \"}\") } } mutation { b }").is_err());
        assert!(ensure_read_only("query { a(s: \"unterminated) }").is_err());
        assert!(ensure_read_only("query { a(s: \"\"\"x) }").is_err());
        assert!(ensure_read_only("query { a ").is_err());
        assert!(ensure_read_only("").is_err());
    }

    fn link_headers(values: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for v in values {
            headers.append(
                HeaderName::from_static("link"),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        headers
    }

    #[test]
    fn parses_next_link() {
        let headers = link_headers(&[
            r#"<https://x/api?page=1>; rel="prev", <https://x/api?page=3&a=b>; rel="next", <https://x/api?page=9>; rel="last""#,
        ]);
        assert_eq!(
            next_link(&headers).unwrap().as_str(),
            "https://x/api?page=3&a=b"
        );

        let headers = link_headers(&[
            "<https://x/1>; rel=first",
            "<https://x/2>; rel=\"next last\"",
        ]);
        assert_eq!(next_link(&headers).unwrap().as_str(), "https://x/2");

        assert!(next_link(&link_headers(&[r#"<https://x/1>; rel="last""#])).is_none());
        assert!(next_link(&HeaderMap::new()).is_none());
    }

    #[test]
    fn extracts_error_details() {
        assert_eq!(
            error_detail(br#"{"message":"401 Unauthorized"}"#),
            ": 401 Unauthorized"
        );
        assert_eq!(
            error_detail(br#"{"message":{"ref":["bad"]}}"#),
            r#": {"ref":["bad"]}"#
        );
        assert_eq!(error_detail(b"<html>nope</html>"), "");
        assert_eq!(error_detail(b"plain"), ": plain");
        assert_eq!(error_detail(b""), "");
    }
}
