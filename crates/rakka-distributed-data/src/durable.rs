//! Durable storage backend for the Replicator. Phase 8.F.
//!
//! akka.net pairs the Replicator with a `DurableStore` that flushes
//! CRDT state to disk so a node can rejoin the cluster without losing
//! local writes. Backends in upstream are LMDB / H2 / SQLite.
//!
//! For rakka we ship two reference impls:
//!
//! * [`NoopDurableStore`] — the default; everything stays in memory.
//! * [`FileDurableStore`] — a small append-only log of `(key, blob)`
//!   markers under a directory, suitable for tests and small workloads.
//!   The on-disk format is `<key>=<base64-of-blob>\n`; we use the
//!   built-in `base64` formatting via hex to keep external deps to zero.
//!
//! Heavier-weight backends (`redb`, `lmdb`) plug in by implementing
//! [`DurableStore`] in a separate crate; the trait surface intentionally
//! mirrors `Replicator::update` / `delete` so the actor (Phase 8.E) can
//! call into it without knowing the on-disk layout.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::Mutex;

/// Abstraction over a durable backing store. Methods are sync so they
/// can be called from anywhere (including the replicator actor task);
/// implementations should keep work small or punt to a worker thread.
pub trait DurableStore: Send + Sync + 'static {
    /// Note that `key` was written. `blob` is the serialized CRDT
    /// snapshot — opaque to the store. Implementations may de-duplicate.
    fn persist(&self, key: &str, blob: &[u8]) -> io::Result<()>;

    /// Convenience: persist a key without a blob (replicator-actor
    /// uses this when the value is type-erased; the user typically
    /// provides a full `persist` via a typed adapter).
    fn persist_marker(&self, key: &str) -> io::Result<()> {
        self.persist(key, &[])
    }

    /// Forget `key`.
    fn delete_marker(&self, key: &str) -> io::Result<()>;

    /// Read the full snapshot for `key`. `None` if absent.
    fn load(&self, key: &str) -> io::Result<Option<Vec<u8>>>;

    /// All keys currently held.
    fn keys(&self) -> io::Result<Vec<String>>;
}

/// In-memory no-op implementation. Used when durability is disabled.
pub struct NoopDurableStore;

impl DurableStore for NoopDurableStore {
    fn persist(&self, _key: &str, _blob: &[u8]) -> io::Result<()> {
        Ok(())
    }
    fn delete_marker(&self, _key: &str) -> io::Result<()> {
        Ok(())
    }
    fn load(&self, _key: &str) -> io::Result<Option<Vec<u8>>> {
        Ok(None)
    }
    fn keys(&self) -> io::Result<Vec<String>> {
        Ok(Vec::new())
    }
}

/// Append-only file-backed store. Keys live as `<dir>/<sanitized>.bin`.
pub struct FileDurableStore {
    dir: PathBuf,
    keys: Mutex<HashSet<String>>,
}

impl FileDurableStore {
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let mut keys = HashSet::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                if let Some(stripped) = name.strip_suffix(".bin") {
                    keys.insert(unsanitize(stripped));
                }
            }
        }
        Ok(Self { dir, keys: Mutex::new(keys) })
    }

    /// Convenience for tests: a fresh per-test temporary directory.
    pub fn tmp() -> io::Result<Self> {
        let mut dir = std::env::temp_dir();
        dir.push(format!("rakka-ddata-{}", std::process::id()));
        dir.push(uuid_like());
        Self::open(dir)
    }

    pub fn contains(&self, key: &str) -> bool {
        self.keys.lock().unwrap().contains(key)
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.dir.join(format!("{}.bin", sanitize(key)))
    }
}

impl DurableStore for FileDurableStore {
    fn persist(&self, key: &str, blob: &[u8]) -> io::Result<()> {
        let path = self.path_for(key);
        let mut f = OpenOptions::new().create(true).truncate(true).write(true).open(&path)?;
        f.write_all(blob)?;
        f.sync_data()?;
        self.keys.lock().unwrap().insert(key.to_string());
        Ok(())
    }
    fn delete_marker(&self, key: &str) -> io::Result<()> {
        let path = self.path_for(key);
        if path.exists() {
            fs::remove_file(path)?;
        }
        self.keys.lock().unwrap().remove(key);
        Ok(())
    }
    fn load(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let path = self.path_for(key);
        if !path.exists() {
            return Ok(None);
        }
        let mut buf = Vec::new();
        File::open(path)?.read_to_end(&mut buf)?;
        Ok(Some(buf))
    }
    fn keys(&self) -> io::Result<Vec<String>> {
        let mut v: Vec<String> = self.keys.lock().unwrap().iter().cloned().collect();
        v.sort();
        Ok(v)
    }
}

fn sanitize(key: &str) -> String {
    key.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' }).collect()
}

fn unsanitize(name: &str) -> String {
    name.to_string()
}

fn uuid_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{n:032x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> PathBuf {
        let mut d = std::env::temp_dir();
        d.push(format!("rakka-ddata-test-{}-{}", std::process::id(), name));
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn file_durable_persist_then_load() {
        let s = FileDurableStore::open(dir("p1")).unwrap();
        s.persist("k", b"hello").unwrap();
        assert!(s.contains("k"));
        let v = s.load("k").unwrap().unwrap();
        assert_eq!(v, b"hello");
    }

    #[test]
    fn file_durable_delete_removes_file() {
        let s = FileDurableStore::open(dir("p2")).unwrap();
        s.persist("k", b"x").unwrap();
        s.delete_marker("k").unwrap();
        assert!(!s.contains("k"));
        assert!(s.load("k").unwrap().is_none());
    }

    #[test]
    fn file_durable_keys_listing_is_stable() {
        let s = FileDurableStore::open(dir("p3")).unwrap();
        s.persist("a", b"1").unwrap();
        s.persist("b", b"2").unwrap();
        let mut keys = s.keys().unwrap();
        keys.sort();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn noop_store_loads_nothing() {
        let s = NoopDurableStore;
        s.persist("k", b"x").unwrap();
        assert!(s.load("k").unwrap().is_none());
        assert!(s.keys().unwrap().is_empty());
    }
}
