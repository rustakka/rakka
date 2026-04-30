//! FileIO — read/write files as streams of `Bytes`. akka.net: `Dsl/FileIO.cs`.

use std::io;
use std::path::{Path, PathBuf};

use bytes::{Bytes, BytesMut};
use futures::stream::StreamExt;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::sink::Sink;
use crate::source::Source;

pub struct FileIO;

impl FileIO {
    /// Read a file in chunks of `chunk_size` bytes. akka.net: `FileIO.FromFile`.
    pub fn from_path(path: impl Into<PathBuf>, chunk_size: usize) -> Source<io::Result<Bytes>> {
        let path: PathBuf = path.into();
        let cap = chunk_size.max(512);
        let s = futures::stream::unfold(
            FileState { path, file: None, cap, done: false },
            |mut state| async move {
                if state.done {
                    return None;
                }
                if state.file.is_none() {
                    match File::open(&state.path).await {
                        Ok(f) => state.file = Some(f),
                        Err(e) => {
                            state.done = true;
                            return Some((Err(e), state));
                        }
                    }
                }
                let mut buf = BytesMut::with_capacity(state.cap);
                buf.resize(state.cap, 0);
                let read = state.file.as_mut().unwrap().read(&mut buf).await;
                match read {
                    Ok(0) => None,
                    Ok(n) => {
                        buf.truncate(n);
                        Some((Ok(buf.freeze()), state))
                    }
                    Err(e) => {
                        state.done = true;
                        Some((Err(e), state))
                    }
                }
            },
        )
        .boxed();
        Source { inner: s }
    }

    /// Write every `Bytes` chunk to `path`, truncating any existing file.
    /// akka.net: `FileIO.ToFile`.
    pub async fn to_path(
        source: Source<Bytes>,
        path: impl AsRef<Path>,
    ) -> io::Result<u64> {
        let mut file = File::create(path.as_ref()).await?;
        let mut stream = source.into_boxed();
        let mut written: u64 = 0;
        while let Some(chunk) = stream.next().await {
            file.write_all(&chunk).await?;
            written += chunk.len() as u64;
        }
        file.flush().await?;
        Ok(written)
    }

    /// Same as `to_path`, but consumes a source of `io::Result<Bytes>`.
    pub async fn pipe_to_path(
        source: Source<io::Result<Bytes>>,
        path: impl AsRef<Path>,
    ) -> io::Result<u64> {
        let mut file = File::create(path.as_ref()).await?;
        let mut stream = source.into_boxed();
        let mut written: u64 = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            written += chunk.len() as u64;
        }
        file.flush().await?;
        Ok(written)
    }
}

struct FileState {
    path: PathBuf,
    file: Option<File>,
    cap: usize,
    done: bool,
}

#[allow(dead_code)]
async fn _drain<T: Send + 'static>(s: Source<T>) {
    Sink::ignore(s).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn round_trip_file_read_write() {
        let mut src = NamedTempFile::new().unwrap();
        src.write_all(b"hello world, this is streams").unwrap();
        let path = src.path().to_path_buf();

        let dst = NamedTempFile::new().unwrap();
        let dst_path = dst.path().to_path_buf();
        drop(dst);

        let read = FileIO::from_path(&path, 8);
        let wrote = FileIO::pipe_to_path(read, &dst_path).await.unwrap();
        assert!(wrote > 0);

        let mut contents = Vec::new();
        std::io::Read::read_to_end(
            &mut std::fs::File::open(&dst_path).unwrap(),
            &mut contents,
        )
        .unwrap();
        assert_eq!(contents, b"hello world, this is streams");
    }
}
