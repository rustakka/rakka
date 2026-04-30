//! Length-prefixed framing for `AkkaPdu`. akka.net: `Remote/Transport/Codec`.
//!
//! On the wire each frame is `u32` big-endian length, followed by a
//! bincode-serialized [`AkkaPdu`].

use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::pdu::AkkaPdu;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode: {0}")]
    Encode(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("frame too large ({0} bytes, max {1})")]
    FrameTooLarge(usize, usize),
}

pub fn encode_pdu(pdu: &AkkaPdu) -> Result<Vec<u8>, CodecError> {
    bincode::serde::encode_to_vec(pdu, bincode::config::standard())
        .map_err(|e| CodecError::Encode(e.to_string()))
}

pub fn decode_pdu(bytes: &[u8]) -> Result<AkkaPdu, CodecError> {
    let (pdu, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| CodecError::Decode(e.to_string()))?;
    Ok(pdu)
}

pub async fn write_frame<W: tokio::io::AsyncWrite + Unpin>(
    w: &mut W,
    pdu: &AkkaPdu,
    max_size: usize,
) -> Result<(), CodecError> {
    let bytes = encode_pdu(pdu)?;
    if bytes.len() > max_size {
        return Err(CodecError::FrameTooLarge(bytes.len(), max_size));
    }
    w.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    w.write_all(&bytes).await?;
    w.flush().await?;
    Ok(())
}

pub async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
    max_size: usize,
) -> Result<AkkaPdu, CodecError> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    if n > max_size {
        return Err(CodecError::FrameTooLarge(n, max_size));
    }
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    decode_pdu(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdu::{AssociateInfo, PROTOCOL_VERSION};
    use rakka_core::actor::Address;

    #[test]
    fn roundtrip_associate() {
        let pdu = AkkaPdu::Associate(AssociateInfo {
            origin: Address::remote("akka.tcp", "S", "127.0.0.1", 1234),
            uid: 99,
            cookie: Some("hunter2".into()),
            protocol_version: PROTOCOL_VERSION,
        });
        let bytes = encode_pdu(&pdu).unwrap();
        let back = decode_pdu(&bytes).unwrap();
        assert_eq!(pdu, back);
    }

    #[test]
    fn roundtrip_heartbeat_and_disassociate() {
        for pdu in [
            AkkaPdu::Heartbeat,
            AkkaPdu::Disassociate(crate::pdu::DisassociateReason::Normal),
        ] {
            let bytes = encode_pdu(&pdu).unwrap();
            assert_eq!(decode_pdu(&bytes).unwrap(), pdu);
        }
    }
}
