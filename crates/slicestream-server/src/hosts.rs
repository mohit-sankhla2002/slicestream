//! Host-header allow-list (against DNS rebinding) and the WebSocket Origin
//! check.

use std::sync::Arc;

use axum::extract::Request;
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::config::HttpConfig;

/// Accepted `Host` values: lowercase host (IPv6 in brackets) and an optional
/// port. An entry without a port accepts the host on any port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostAllowList {
    entries: Vec<(String, Option<u16>)>,
}

impl HostAllowList {
    /// From `allowed_hosts`, or, when that is empty, `localhost`,
    /// `127.0.0.1`, `[::1]` and the host of `addr`, each with or without the
    /// port of `addr`.
    pub fn from_config(http: &HttpConfig) -> Self {
        let entries = if http.allowed_hosts.is_empty() {
            let port = Some(http.addr.port());
            let addr_host = match http.addr {
                std::net::SocketAddr::V4(a) => a.ip().to_string(),
                std::net::SocketAddr::V6(a) => format!("[{}]", a.ip()),
            };
            [
                "localhost".to_owned(),
                "127.0.0.1".into(),
                "[::1]".into(),
                addr_host,
            ]
            .into_iter()
            .map(|h| (h, port))
            .collect()
        } else {
            http.allowed_hosts
                .iter()
                .filter_map(|h| {
                    let parsed = split_host_port(h);
                    if parsed.is_none() {
                        tracing::warn!(host = %h, "ignoring invalid allowed_hosts entry");
                    }
                    parsed
                })
                .collect()
        };
        Self { entries }
    }

    /// Whether a `Host` header value is accepted.
    pub fn allows(&self, host: &str) -> bool {
        let Some((host, port)) = split_host_port(host) else {
            return false;
        };
        self.entries.iter().any(|(h, p)| {
            *h == host
                && match (port, p) {
                    (None, _) | (_, None) => true,
                    (Some(a), Some(b)) => a == *b,
                }
        })
    }
}

/// Split `host[:port]` (IPv6 as `[addr][:port]`) and lowercase the host.
fn split_host_port(s: &str) -> Option<(String, Option<u16>)> {
    let s = s.trim();
    let (host, port) = if let Some(rest) = s.strip_prefix('[') {
        let end = rest.find(']')?;
        let host = &s[..end + 2];
        match &rest[end + 1..] {
            "" => (host, None),
            p => (host, Some(p.strip_prefix(':')?)),
        }
    } else {
        match s.split_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (s, None),
        }
    };
    if host.is_empty() || host.contains(['/', '@', ' ']) {
        return None;
    }
    let port = match port {
        Some(p) => Some(p.parse::<u16>().ok()?),
        None => None,
    };
    Some((host.to_ascii_lowercase(), port))
}

/// The request's host: the `Host` header, else the URI authority (HTTP/2).
fn request_host(req: &Request) -> Option<String> {
    if let Some(h) = req.headers().get(header::HOST) {
        return h.to_str().ok().map(str::to_owned);
    }
    req.uri().authority().map(|a| a.as_str().to_owned())
}

/// Middleware: answer 421 Misdirected Request unless the host is allowed.
pub async fn host_guard(allowed: Arc<HostAllowList>, req: Request, next: Next) -> Response {
    match request_host(&req) {
        Some(h) if allowed.allows(&h) => next.run(req).await,
        host => {
            tracing::debug!(?host, "rejecting request for a host not in allowed_hosts");
            StatusCode::MISDIRECTED_REQUEST.into_response()
        }
    }
}

/// Whether a WebSocket upgrade may proceed: no `Origin` (not a browser), the
/// page's own origin (same host and port as `Host`), or a configured
/// `cors_allowed_origins` entry. Browsers do not apply CORS to WebSockets,
/// so this check stands in for it on the stream route.
pub fn websocket_origin_allowed(http: &HttpConfig, headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let Ok(origin) = origin.to_str() else {
        return false;
    };
    if http.cors_allowed_origins.iter().any(|o| o == origin) {
        return true;
    }
    let origin_host = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"));
    let host = headers.get(header::HOST).and_then(|h| h.to_str().ok());
    match (origin_host, host) {
        (Some(o), Some(h)) => o.eq_ignore_ascii_case(h),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split() {
        assert_eq!(
            split_host_port("LocalHost"),
            Some(("localhost".into(), None))
        );
        assert_eq!(
            split_host_port("a.example:80"),
            Some(("a.example".into(), Some(80)))
        );
        assert_eq!(
            split_host_port("[::1]:8080"),
            Some(("[::1]".into(), Some(8080)))
        );
        assert_eq!(split_host_port("[::1]"), Some(("[::1]".into(), None)));
        assert_eq!(split_host_port("a:b"), None);
        assert_eq!(split_host_port("::1"), None);
        assert_eq!(split_host_port("[::1]x"), None);
        assert_eq!(split_host_port(""), None);
    }

    #[test]
    fn explicit_entries() {
        let http = HttpConfig {
            allowed_hosts: vec!["pacs.example".into(), "viewer.example:8443".into()],
            ..HttpConfig::default()
        };
        let a = HostAllowList::from_config(&http);
        assert!(a.allows("pacs.example"));
        assert!(a.allows("PACS.example:1234"));
        assert!(a.allows("viewer.example:8443"));
        assert!(a.allows("viewer.example"));
        assert!(!a.allows("viewer.example:8444"));
        assert!(!a.allows("localhost"));
    }

    #[test]
    fn websocket_origin() {
        let mut headers = HeaderMap::new();
        let http = HttpConfig {
            cors_allowed_origins: vec!["https://viewer.example".into()],
            ..HttpConfig::default()
        };
        headers.insert(header::HOST, "127.0.0.1:8080".parse().unwrap());
        assert!(websocket_origin_allowed(&http, &headers));
        headers.insert(header::ORIGIN, "http://127.0.0.1:8080".parse().unwrap());
        assert!(websocket_origin_allowed(&http, &headers));
        headers.insert(header::ORIGIN, "https://viewer.example".parse().unwrap());
        assert!(websocket_origin_allowed(&http, &headers));
        headers.insert(header::ORIGIN, "https://evil.example".parse().unwrap());
        assert!(!websocket_origin_allowed(&http, &headers));
        headers.insert(header::ORIGIN, "null".parse().unwrap());
        assert!(!websocket_origin_allowed(&http, &headers));
    }
}
