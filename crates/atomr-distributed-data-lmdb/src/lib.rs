//! redb-backed [`DurableStore`] for atomr distributed-data.
//!
//! akka.net analog: `Akka.DistributedData.LightningDB.LmdbDurableStore`.
//! atomr substitutes `redb` (a pure-Rust embedded KV store with the
//! same single-writer / multi-reader / mmap semantics as LMDB) so the
//! crate builds without a system C dependency.
//!
//! Example:
//!
//! ```no_run
//! use atomr_distributed_data::DurableStore;
//! use atomr_distributed_data_lmdb::RedbDurableStore;
//!
//! let store = RedbDurableStore::open("./ddata.redb").unwrap();
//! store.persist("counter", b"snapshot").unwrap();
//! ```
//!
//! All `DurableStore` operations are synchronous and run inline on the
//! caller's thread — appropriate for the replicator-actor cell where
//! writes are infrequent and small.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use atomr_distributed_data::DurableStore;
use redb::{Database, ReadableTable, TableDefinition};
use thiserror::Error;

const TABLE: TableDefinition<'static, &str, Vec<u8>> = TableDefinition::new("ddata-snapshots");

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RedbError {
    #[error("redb open: {0}")]
    Open(String),
    #[error("redb txn: {0}")]
    Txn(String),
}

impl From<RedbError> for io::Error {
    fn from(e: RedbError) -> Self {
        io::Error::new(io::ErrorKind::Other, e.to_string())
    }
}

/// Durable store backed by a single-file redb database.
///
/// Cheap to clone; internally an `Arc<Database>`.
pub struct RedbDurableStore {
    db: Arc<Database>,
    path: PathBuf,
}

impl RedbDurableStore {
    /// Open or create the database file at `path`.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, RedbError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| RedbError::Open(e.to_string()))?;
        }
        let db = Database::create(&path).map_err(|e| RedbError::Open(e.to_string()))?;
        // Ensure the table exists by opening and committing an empty
        // write txn — first run on a new file would otherwise refuse
        // a read txn against a non-existent table.
        {
            let w = db.begin_write().map_err(|e| RedbError::Txn(e.to_string()))?;
            {
                let _ = w.open_table(TABLE).map_err(|e| RedbError::Txn(e.to_string()))?;
            }
            w.commit().map_err(|e| RedbError::Txn(e.to_string()))?;
        }
        Ok(Self { db: Arc::new(db), path })
    }

    /// Convenience: a fresh per-test temporary database file.
    pub fn tmp() -> Result<Self, RedbError> {
        let mut p = std::env::temp_dir();
        p.push(format!("atomr-ddata-redb-{}-{}.redb", std::process::id(), uuid_like()));
        Self::open(p)
    }

    /// On-disk path the database lives at.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl DurableStore for RedbDurableStore {
    fn persist(&self, key: &str, blob: &[u8]) -> io::Result<()> {
        let txn = self.db.begin_write().map_err(|e| RedbError::Txn(e.to_string()))?;
        {
            let mut t = txn.open_table(TABLE).map_err(|e| RedbError::Txn(e.to_string()))?;
            t.insert(key, blob.to_vec()).map_err(|e| RedbError::Txn(e.to_string()))?;
        }
        txn.commit().map_err(|e| RedbError::Txn(e.to_string()))?;
        Ok(())
    }

    fn delete_marker(&self, key: &str) -> io::Result<()> {
        let txn = self.db.begin_write().map_err(|e| RedbError::Txn(e.to_string()))?;
        {
            let mut t = txn.open_table(TABLE).map_err(|e| RedbError::Txn(e.to_string()))?;
            t.remove(key).map_err(|e| RedbError::Txn(e.to_string()))?;
        }
        txn.commit().map_err(|e| RedbError::Txn(e.to_string()))?;
        Ok(())
    }

    fn load(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let txn = self.db.begin_read().map_err(|e| RedbError::Txn(e.to_string()))?;
        let t = txn.open_table(TABLE).map_err(|e| RedbError::Txn(e.to_string()))?;
        let v = t.get(key).map_err(|e| RedbError::Txn(e.to_string()))?;
        Ok(v.map(|g| g.value()))
    }

    fn keys(&self) -> io::Result<Vec<String>> {
        let txn = self.db.begin_read().map_err(|e| RedbError::Txn(e.to_string()))?;
        let t = txn.open_table(TABLE).map_err(|e| RedbError::Txn(e.to_string()))?;
        let mut out = Vec::new();
        for entry in t.iter().map_err(|e| RedbError::Txn(e.to_string()))? {
            let (k, _) = entry.map_err(|e| RedbError::Txn(e.to_string()))?;
            out.push(k.value().to_string());
        }
        out.sort();
        Ok(out)
    }
}

fn uuid_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{n:032x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fresh_store() -> (TempDir, RedbDurableStore) {
        let td = TempDir::new().unwrap();
        let store = RedbDurableStore::open(td.path().join("ddata.redb")).unwrap();
        (td, store)
    }

    #[test]
    fn persist_then_load_roundtrip() {
        let (_td, s) = fresh_store();
        s.persist("counter", b"hello").unwrap();
        let got = s.load("counter").unwrap().unwrap();
        assert_eq!(got, b"hello");
    }

    #[test]
    fn load_returns_none_for_unknown_key() {
        let (_td, s) = fresh_store();
        assert!(s.load("missing").unwrap().is_none());
    }

    #[test]
    fn delete_marker_removes_key() {
        let (_td, s) = fresh_store();
        s.persist("k", b"v").unwrap();
        s.delete_marker("k").unwrap();
        assert!(s.load("k").unwrap().is_none());
    }

    #[test]
    fn delete_unknown_key_is_ok() {
        let (_td, s) = fresh_store();
        s.delete_marker("never").unwrap();
    }

    #[test]
    fn persist_overwrites_existing_value() {
        let (_td, s) = fresh_store();
        s.persist("k", b"old").unwrap();
        s.persist("k", b"new").unwrap();
        assert_eq!(s.load("k").unwrap().unwrap(), b"new");
    }

    #[test]
    fn keys_returns_sorted_distinct() {
        let (_td, s) = fresh_store();
        s.persist("zeta", b"a").unwrap();
        s.persist("alpha", b"a").unwrap();
        s.persist("mu", b"a").unwrap();
        let keys = s.keys().unwrap();
        assert_eq!(keys, vec!["alpha".to_string(), "mu".to_string(), "zeta".to_string()]);
    }

    #[test]
    fn data_survives_reopen() {
        let td = TempDir::new().unwrap();
        let path = td.path().join("ddata.redb");
        {
            let s = RedbDurableStore::open(&path).unwrap();
            s.persist("durable", b"persisted-bytes").unwrap();
        }
        // Drop the first store, reopen the same file.
        let s2 = RedbDurableStore::open(&path).unwrap();
        let v = s2.load("durable").unwrap().unwrap();
        assert_eq!(v, b"persisted-bytes");
    }

    #[test]
    fn tmp_uses_unique_paths() {
        let a = RedbDurableStore::tmp().unwrap();
        let b = RedbDurableStore::tmp().unwrap();
        assert_ne!(a.path(), b.path());
    }

    #[test]
    fn persist_marker_works_with_empty_blob() {
        let (_td, s) = fresh_store();
        s.persist_marker("flag").unwrap();
        let v = s.load("flag").unwrap().unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn delete_then_load_yields_none() {
        let (_td, s) = fresh_store();
        s.persist("k", b"x").unwrap();
        s.delete_marker("k").unwrap();
        assert!(s.load("k").unwrap().is_none());
        assert!(s.keys().unwrap().is_empty());
    }
}
