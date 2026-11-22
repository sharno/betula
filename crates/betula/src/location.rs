//! Opening an object store from a URL.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path;

use crate::error::{Error, Result};

/// Environment variables passed to the store builder (lowercased, as
/// `object_store` expects): credentials, regions, endpoints, and so on.
const ENV_PREFIXES: [&str; 3] = ["AWS_", "GOOGLE_", "AZURE_"];

/// The store and the database path within it for a URL like
/// `s3://bucket/path`, `gs://bucket/path`, `az://container/path`,
/// `file:///abs/path` or `memory:///`.
pub fn parse(url: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
    let parsed = url::Url::parse(url).map_err(|e| Error::InvalidConfig(format!("{url}: {e}")))?;
    let options = std::env::vars()
        .filter(|(k, _)| ENV_PREFIXES.iter().any(|p| k.starts_with(p)))
        .map(|(k, v)| (k.to_ascii_lowercase(), v));
    let (store, path) = object_store::parse_url_opts(&parsed, options)
        .map_err(|e| Error::InvalidConfig(format!("{url}: {e}")))?;
    Ok((Arc::from(store), path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_memory_and_rejects_garbage() {
        let (_, path) = parse("memory:///dbs/one").unwrap();
        assert_eq!(path.as_ref(), "dbs/one");
        assert!(matches!(parse("not a url"), Err(Error::InvalidConfig(_))));
    }
}
