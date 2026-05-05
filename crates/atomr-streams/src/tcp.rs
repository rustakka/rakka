//! TCP stream helpers.
//!
//! * [`Tcp::outgoing_connection`] — connect to `addr` and expose the remote
//!   side as a `(Sink<Bytes>, Source<io::Result<Bytes>>)` pair.
//! * [`Tcp::bind`] — accept inbound connections as a stream of
//!   [`IncomingConnection`]s.

use std::io;
use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use futures::stream::StreamExt;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::source::Source;

pub struct Tcp;

pub struct OutgoingConnection {
    pub reader: Source<io::Result<Bytes>>,
    pub writer: tokio::sync::mpsc::UnboundedSender<Bytes>,
    pub remote_addr: SocketAddr,
}

pub struct IncomingConnection {
    pub reader: Source<io::Result<Bytes>>,
    pub writer: tokio::sync::mpsc::UnboundedSender<Bytes>,
    pub remote_addr: SocketAddr,
    pub local_addr: SocketAddr,
}

impl Tcp {
    pub async fn outgoing_connection(addr: SocketAddr) -> io::Result<OutgoingConnection> {
        let stream = TcpStream::connect(addr).await?;
        let remote_addr = stream.peer_addr()?;
        let (rd, mut wr) = split(stream);
        let (w_tx, mut w_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
        tokio::spawn(async move {
            while let Some(b) = w_rx.recv().await {
                if wr.write_all(&b).await.is_err() {
                    break;
                }
            }
            let _ = wr.shutdown().await;
        });
        let reader = read_stream(rd);
        Ok(OutgoingConnection { reader, writer: w_tx, remote_addr })
    }

    pub async fn bind(addr: SocketAddr) -> io::Result<Source<io::Result<IncomingConnection>>> {
        let listener = TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        let s = futures::stream::unfold(AcceptState { listener, local }, |state| async move {
            match state.listener.accept().await {
                Ok((stream, remote)) => {
                    let (rd, mut wr) = split(stream);
                    let (w_tx, mut w_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
                    tokio::spawn(async move {
                        while let Some(b) = w_rx.recv().await {
                            if wr.write_all(&b).await.is_err() {
                                break;
                            }
                        }
                        let _ = wr.shutdown().await;
                    });
                    let reader = read_stream(rd);
                    let ic = IncomingConnection {
                        reader,
                        writer: w_tx,
                        remote_addr: remote,
                        local_addr: state.local,
                    };
                    Some((Ok(ic), state))
                }
                Err(e) => Some((Err(e), state)),
            }
        })
        .boxed();
        Ok(Source { inner: s })
    }
}

struct AcceptState {
    listener: TcpListener,
    local: SocketAddr,
}

fn read_stream<R>(rd: R) -> Source<io::Result<Bytes>>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    struct St<R> {
        rd: R,
        done: bool,
    }
    let s = futures::stream::unfold(St { rd, done: false }, |mut st| async move {
        if st.done {
            return None;
        }
        let mut buf = BytesMut::with_capacity(4096);
        buf.resize(4096, 0);
        match st.rd.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((Ok(buf.freeze()), st))
            }
            Err(e) => {
                st.done = true;
                Some((Err(e), st))
            }
        }
    })
    .boxed();
    Source { inner: s }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;

    #[tokio::test]
    async fn tcp_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // rebind using Tcp::bind

        let incoming = Tcp::bind(addr).await.unwrap();
        let (tx_done, mut rx_done) = tokio::sync::mpsc::unbounded_channel::<Vec<Bytes>>();
        tokio::spawn(async move {
            let mut stream = incoming.into_boxed();
            if let Some(Ok(conn)) = stream.next().await {
                let collected = Sink::collect(conn.reader).await;
                let mut ok = Vec::new();
                for b in collected.into_iter().flatten() {
                    ok.push(b);
                }
                let _ = tx_done.send(ok);
            }
        });

        let out = Tcp::outgoing_connection(addr).await.unwrap();
        out.writer.send(Bytes::from_static(b"hello")).unwrap();
        drop(out.writer);

        let received = rx_done.recv().await.unwrap();
        assert!(received.iter().any(|b| b.as_ref() == b"hello"));
    }
}
