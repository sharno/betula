use std::future::Future;
use std::time::Duration;

use crate::error::{Error, Result};

const ATTEMPTS: u32 = 8;
const BASE: Duration = Duration::from_millis(50);
const CAP: Duration = Duration::from_secs(5);

/// Retries object store failures with capped exponential backoff. Only safe
/// for idempotent operations (put-if-absent calls compare on conflict).
pub async fn retry<T, F, Fut>(what: &str, mut op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Err(Error::ObjectStore(e)) if attempt + 1 < ATTEMPTS => {
                let delay = (BASE * 2u32.pow(attempt)).min(CAP);
                tracing::warn!(%e, attempt, ?delay, "{what} failed, retrying");
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            other => return other,
        }
    }
}
