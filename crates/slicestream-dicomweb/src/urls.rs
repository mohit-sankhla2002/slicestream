//! Absolute URLs for Retrieve URL attributes and BulkDataURIs.

/// The server's public base URL, without a trailing slash.
///
/// Uses `public_url` when set. Otherwise uses the `Host` header, if it is
/// non-empty, made only of visible ASCII, and free of `\"`, `\\` and `/`
/// (URLs are spliced into JSON). Otherwise uses `fallback_host`.
pub fn base_url(public_url: Option<&str>, host: Option<&str>, fallback_host: &str) -> String {
    if let Some(p) = public_url {
        let p = p.trim_end_matches('/');
        if !p.is_empty() {
            return p.to_owned();
        }
    }
    let host = host
        .filter(|h| {
            !h.is_empty()
                && h.bytes()
                    .all(|b| b.is_ascii_graphic() && !matches!(b, b'"' | b'\\' | b'/'))
        })
        .unwrap_or(fallback_host);
    format!("http://{host}")
}

pub fn study_url(base: &str, study: &str) -> String {
    format!("{base}/dicom-web/studies/{study}")
}

pub fn series_url(base: &str, study: &str, series: &str) -> String {
    format!("{}/series/{series}", study_url(base, study))
}

pub fn instance_url(base: &str, study: &str, series: &str, sop: &str) -> String {
    format!("{}/instances/{sop}", series_url(base, study, series))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_url_public_trailing_slash() {
        assert_eq!(
            base_url(Some("https://pacs.example.org//"), Some("h:1"), "0.0.0.0:1"),
            "https://pacs.example.org"
        );
    }

    #[test]
    fn base_url_from_host() {
        assert_eq!(
            base_url(None, Some("pacs:8042"), "fb:1"),
            "http://pacs:8042"
        );
        assert_eq!(base_url(None, Some(""), "fb:1"), "http://fb:1");
        assert_eq!(base_url(None, Some("a b"), "fb:1"), "http://fb:1");
        assert_eq!(base_url(None, None, "fb:1"), "http://fb:1");
    }

    #[test]
    fn base_url_rejects_unsafe_host() {
        for h in ["x\"y", "x\\y", "x/y", "x\",\"k\":\"v", "a b", "a\tb"] {
            assert_eq!(base_url(None, Some(h), "fb:1"), "http://fb:1", "{h}");
        }
    }

    #[test]
    fn retrieve_urls() {
        assert_eq!(
            instance_url("http://h", "1", "2", "3"),
            "http://h/dicom-web/studies/1/series/2/instances/3"
        );
        assert_eq!(
            series_url("http://h", "1", "2"),
            "http://h/dicom-web/studies/1/series/2"
        );
        assert_eq!(study_url("http://h", "1"), "http://h/dicom-web/studies/1");
    }
}
