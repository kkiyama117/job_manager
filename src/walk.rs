//! Walk `<root>/*` for `flow.toml` files and yield parsed `JobFlow`s.
//!
//! Uses tokio `spawn_blocking` to parallelize blocking file I/O without
//! tying up the async runtime threads.

use std::path::{Path, PathBuf};

use async_stream::stream;
use futures::stream::{self, Stream, StreamExt};
use gaussian_job_shared::entities::workflow::JobFlow;

use crate::concurrency::parallelism;
use crate::error::JobManagerError;
use crate::flow_io::read_flow;

/// Return paths to all candidate `flow.toml` files (synchronous; cheap).
fn candidate_paths(root: &Path) -> Result<Vec<PathBuf>, JobManagerError> {
    let mut out = Vec::new();
    let read_dir = std::fs::read_dir(root).map_err(|source| JobManagerError::Io {
        path: root.to_path_buf(),
        source,
    })?;
    for entry in read_dir {
        let entry = entry.map_err(|source| JobManagerError::Io {
            path: root.to_path_buf(),
            source,
        })?;
        let p = entry.path();
        if !p.is_dir() {
            continue;
        }
        let candidate = p.join("flow.toml");
        if candidate.is_file() {
            out.push(candidate);
        }
    }
    Ok(out)
}

/// Stream `JobFlow`s parsed from each `<root>/<uuid>/flow.toml`.
/// Skips entries with no `flow.toml`. Malformed TOML surfaces as a stream
/// item `Err(JobManagerError::TomlParse{..})`.
pub fn walk_flows(
    root: &Path,
) -> impl Stream<Item = Result<JobFlow, JobManagerError>> + Send + 'static {
    let root = root.to_path_buf();
    let parallelism = parallelism();
    stream! {
        let paths = match candidate_paths(&root) {
            Ok(p) => p,
            Err(e) => {
                yield Err(e);
                return;
            }
        };
        let body = stream::iter(paths)
            .map(|p| async move {
                tokio::task::spawn_blocking(move || read_flow(&p))
                    .await
                    .map_err(|e| JobManagerError::Other(format!("spawn_blocking join: {e}")))?
            })
            .buffer_unordered(parallelism);
        let mut body = std::pin::pin!(body);
        while let Some(r) = body.next().await {
            yield r;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow_io::write_flow;
    use chrono::Utc;
    use futures::StreamExt;
    use std::collections::BTreeMap;
    use tempfile::TempDir;
    use uuid::Uuid;

    fn empty_flow(uuid: Uuid) -> JobFlow {
        JobFlow {
            uuid,
            created_at: Utc::now(),
            tags: BTreeMap::new(),
            jobs: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn walk_empty_root_yields_nothing() {
        let dir = TempDir::new().unwrap();
        let s = walk_flows(dir.path());
        let v: Vec<_> = s.collect().await;
        assert!(v.is_empty());
    }

    #[tokio::test]
    async fn walk_returns_each_jobflow_exactly_once() {
        let dir = TempDir::new().unwrap();
        let mut expected = Vec::new();
        for _ in 0..5 {
            let u = Uuid::now_v7();
            let f = empty_flow(u);
            let p = dir.path().join(u.to_string()).join("flow.toml");
            write_flow(&p, &f).unwrap();
            expected.push(u);
        }
        let s = walk_flows(dir.path());
        let mut found: Vec<Uuid> = s
            .filter_map(|r| async move { r.ok().map(|f| f.uuid) })
            .collect()
            .await;
        found.sort();
        let mut expected_sorted = expected.clone();
        expected_sorted.sort();
        assert_eq!(found, expected_sorted);
    }

    #[tokio::test]
    async fn walk_skips_dirs_with_no_flow_toml() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("orphan")).unwrap();
        let s = walk_flows(dir.path());
        let v: Vec<_> = s.collect().await;
        assert!(v.is_empty());
    }

    #[tokio::test]
    async fn walk_surfaces_parse_error_as_stream_item() {
        let dir = TempDir::new().unwrap();
        let bad = dir.path().join("badflow").join("flow.toml");
        std::fs::create_dir_all(bad.parent().unwrap()).unwrap();
        std::fs::write(&bad, "this = ::not valid toml::").unwrap();
        let s = walk_flows(dir.path());
        let v: Vec<_> = s.collect().await;
        assert_eq!(v.len(), 1);
        assert!(v[0].is_err());
    }
}
