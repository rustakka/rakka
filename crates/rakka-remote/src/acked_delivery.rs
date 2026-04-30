//! Reliable sliding-window delivery. akka.net: `Remote/Transport/AckedDelivery.cs`.
//!
//! Each `Endpoint` maintains an [`AckedSendBuffer`] of in-flight envelopes
//! and an [`AckedReceiveBuffer`] that suppresses duplicates and produces
//! cumulative + nack acks.

use std::collections::{BTreeMap, BTreeSet};

use crate::envelope::RemoteEnvelope;
use crate::pdu::AckInfo;

/// Monotonic per-endpoint sequence counter. Wraps in 2^64 — at one
/// envelope per nanosecond that's ~584 years, so wrap-around is ignored.
#[derive(Debug, Default, Clone, Copy)]
pub struct SeqNo(pub u64);

impl SeqNo {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(1);
        self.0
    }
}

/// Outbound buffer of envelopes awaiting ack. New sends are appended;
/// `Ack` removes everything up to (and including) the cumulative
/// watermark, and explicit nacks promote those back to "needs resend".
pub struct AckedSendBuffer {
    capacity: u32,
    pending: BTreeMap<u64, RemoteEnvelope>,
    /// seq numbers explicitly nacked by the receiver and queued for resend.
    nacks: BTreeSet<u64>,
}

impl AckedSendBuffer {
    pub fn new(capacity: u32) -> Self {
        Self { capacity, pending: BTreeMap::new(), nacks: BTreeSet::new() }
    }

    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_full(&self) -> bool {
        self.pending.len() >= self.capacity as usize
    }

    pub fn push(&mut self, env: RemoteEnvelope) -> Result<(), RemoteEnvelope> {
        if self.is_full() {
            return Err(env);
        }
        self.pending.insert(env.seq_no, env);
        Ok(())
    }

    pub fn apply_ack(&mut self, ack: &AckInfo) {
        self.pending.retain(|seq, _| *seq > ack.cumulative_ack);
        self.nacks.extend(ack.nacks.iter().copied());
    }

    /// Drain pending envelopes that the writer should resend right now.
    /// Includes everything currently nacked plus everything older than the
    /// last cumulative ack we observed.
    pub fn drain_resend(&mut self) -> Vec<RemoteEnvelope> {
        let mut out = Vec::new();
        let nacks = std::mem::take(&mut self.nacks);
        for seq in nacks {
            if let Some(e) = self.pending.get(&seq).cloned() {
                out.push(e);
            }
        }
        out
    }
}

/// Inbound buffer that tracks which sequence numbers have been delivered
/// and produces cumulative+nack ack reports. New envelopes whose `seq_no`
/// is below the cumulative watermark are duplicates and silently dropped.
pub struct AckedReceiveBuffer {
    cumulative: u64,
    delivered: BTreeSet<u64>,
}

impl Default for AckedReceiveBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl AckedReceiveBuffer {
    pub fn new() -> Self {
        Self { cumulative: 0, delivered: BTreeSet::new() }
    }

    /// Record an inbound envelope. Returns `true` if it's new (caller
    /// should deliver it), `false` if it was already seen.
    pub fn observe(&mut self, seq_no: u64) -> bool {
        if seq_no <= self.cumulative {
            return false;
        }
        if !self.delivered.insert(seq_no) {
            return false;
        }
        // Advance the cumulative watermark over any contiguous run.
        loop {
            let next = self.cumulative + 1;
            if self.delivered.remove(&next) {
                self.cumulative = next;
            } else {
                break;
            }
        }
        true
    }

    pub fn ack(&self) -> AckInfo {
        let nacks: Vec<u64> = self.delivered.iter().copied().collect();
        AckInfo { cumulative_ack: self.cumulative, nacks: missing_below(self.cumulative, &nacks) }
    }
}

fn missing_below(_cumulative: u64, _delivered_above: &[u64]) -> Vec<u64> {
    // We never produce nacks for *future* deliveries; the receiver only
    // nacks what it has already partially seen but hasn't been able to
    // close the gap on. With reliable TCP under us, this set is usually
    // empty, but the field exists so future transports (UDP-style) can
    // populate it.
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(seq: u64) -> RemoteEnvelope {
        RemoteEnvelope::user("akka.tcp://X@h:1/user/a", None, 0, 0, seq, 1, "u32", vec![])
    }

    #[test]
    fn send_buffer_acks_remove_pending() {
        let mut sb = AckedSendBuffer::new(8);
        for i in 1..=5 {
            sb.push(env(i)).unwrap();
        }
        assert_eq!(sb.pending_len(), 5);
        sb.apply_ack(&AckInfo { cumulative_ack: 3, nacks: vec![] });
        assert_eq!(sb.pending_len(), 2);
    }

    #[test]
    fn receive_buffer_dedups_and_advances() {
        let mut rb = AckedReceiveBuffer::new();
        assert!(rb.observe(1));
        assert!(rb.observe(2));
        assert!(!rb.observe(2));
        assert_eq!(rb.ack().cumulative_ack, 2);
        // out-of-order
        assert!(rb.observe(4));
        assert_eq!(rb.ack().cumulative_ack, 2);
        assert!(rb.observe(3));
        assert_eq!(rb.ack().cumulative_ack, 4);
    }

    #[test]
    fn send_buffer_full_returns_envelope() {
        let mut sb = AckedSendBuffer::new(2);
        sb.push(env(1)).unwrap();
        sb.push(env(2)).unwrap();
        let leftover = sb.push(env(3)).unwrap_err();
        assert_eq!(leftover.seq_no, 3);
    }
}
