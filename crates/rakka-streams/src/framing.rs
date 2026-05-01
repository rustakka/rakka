//! Framing utilities — split a byte stream into messages.
//! akka.net: `Dsl/Framing.cs`, `Dsl/JsonFraming.cs`.

use bytes::{Buf, Bytes, BytesMut};
use futures::stream::{BoxStream, StreamExt};
use thiserror::Error;

use crate::flow::Flow;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FramingError {
    #[error("frame exceeds {0} bytes")]
    FrameTooLarge(usize),
    #[error("truncated frame at end of stream")]
    Truncated,
}

pub struct Framing;

struct FrameState<S> {
    stream: S,
    buf: BytesMut,
    done: bool,
}

impl Framing {
    /// Split an incoming `Bytes` stream using a single-byte delimiter, dropping
    /// the delimiter from each produced frame. akka.net: `Framing.Delimiter`.
    pub fn delimiter(
        delimiter: u8,
        max_frame_length: usize,
    ) -> Flow<Bytes, Result<Bytes, FramingError>> {
        Flow {
            transform: Box::new(move |stream: BoxStream<'static, Bytes>| {
                futures::stream::unfold(
                    FrameState { stream, buf: BytesMut::new(), done: false },
                    move |mut st| async move {
                        if st.done {
                            return None;
                        }
                        loop {
                            if let Some(pos) = st.buf.iter().position(|b| *b == delimiter) {
                                let frame = st.buf.split_to(pos).freeze();
                                st.buf.advance(1);
                                if frame.len() > max_frame_length {
                                    st.done = true;
                                    return Some((
                                        Err(FramingError::FrameTooLarge(max_frame_length)),
                                        st,
                                    ));
                                }
                                return Some((Ok(frame), st));
                            }
                            match st.stream.next().await {
                                Some(chunk) => {
                                    st.buf.extend_from_slice(&chunk);
                                    if st.buf.len() > max_frame_length {
                                        st.done = true;
                                        return Some((
                                            Err(FramingError::FrameTooLarge(max_frame_length)),
                                            st,
                                        ));
                                    }
                                }
                                None => {
                                    if st.buf.is_empty() {
                                        return None;
                                    }
                                    st.done = true;
                                    return Some((Err(FramingError::Truncated), st));
                                }
                            }
                        }
                    },
                )
                .boxed()
            }),
        }
    }

    /// Split by length-prefixed frames. The prefix is a little-endian u32
    /// giving the size of the payload. akka.net: `Framing.LengthField`.
    pub fn length_field(
        max_frame_length: usize,
    ) -> Flow<Bytes, Result<Bytes, FramingError>> {
        Flow {
            transform: Box::new(move |stream: BoxStream<'static, Bytes>| {
                futures::stream::unfold(
                    FrameState { stream, buf: BytesMut::new(), done: false },
                    move |mut st| async move {
                        if st.done {
                            return None;
                        }
                        loop {
                            if st.buf.len() >= 4 {
                                let len = u32::from_le_bytes(st.buf[..4].try_into().unwrap())
                                    as usize;
                                if len > max_frame_length {
                                    st.done = true;
                                    return Some((
                                        Err(FramingError::FrameTooLarge(max_frame_length)),
                                        st,
                                    ));
                                }
                                if st.buf.len() >= 4 + len {
                                    st.buf.advance(4);
                                    let frame = st.buf.split_to(len).freeze();
                                    return Some((Ok(frame), st));
                                }
                            }
                            match st.stream.next().await {
                                Some(chunk) => st.buf.extend_from_slice(&chunk),
                                None => {
                                    if st.buf.is_empty() {
                                        return None;
                                    }
                                    st.done = true;
                                    return Some((Err(FramingError::Truncated), st));
                                }
                            }
                        }
                    },
                )
                .boxed()
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;
    use crate::source::Source;

    #[tokio::test]
    async fn delimiter_framing_splits_chunks() {
        let source = Source::from_iter(vec![
            Bytes::from_static(b"hello\nwo"),
            Bytes::from_static(b"rld\nfoo\n"),
        ]);
        let framed = source.via(Framing::delimiter(b'\n', 1024));
        let out: Vec<_> = Sink::collect(framed).await;
        let ok: Vec<_> = out.into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(
            ok,
            vec![
                Bytes::from_static(b"hello"),
                Bytes::from_static(b"world"),
                Bytes::from_static(b"foo"),
            ]
        );
    }

    #[tokio::test]
    async fn length_field_framing_handles_splits() {
        let mut buf = Vec::new();
        let msgs: [&[u8]; 2] = [b"abc", b"hello"];
        for m in msgs {
            buf.extend_from_slice(&(m.len() as u32).to_le_bytes());
            buf.extend_from_slice(m);
        }
        let source = Source::from_iter(vec![
            Bytes::copy_from_slice(&buf[..5]),
            Bytes::copy_from_slice(&buf[5..]),
        ]);
        let framed = source.via(Framing::length_field(1024));
        let out: Vec<_> = Sink::collect(framed).await;
        let ok: Vec<_> = out.into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(ok, vec![Bytes::from_static(b"abc"), Bytes::from_static(b"hello")]);
    }
}
