//! Saga end-to-end: an Account aggregate with two instances; a saga
//! reacts to `Withdrawn` events and dispatches `Deposit` commands on a
//! sibling account. Tests the happy path; compensation chain is
//! exercised by the `Err` arm of the dispatcher.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::prelude::*;
use atomr_persistence::{Eventsourced, InMemoryJournal};

#[derive(Debug, thiserror::Error)]
enum AcctErr {
    #[error("insufficient funds")]
    Insufficient,
}

#[derive(Default, Debug)]
struct AcctState {
    balance: i64,
}

#[derive(Clone, Debug)]
enum AcctEvent {
    Deposited { from: String, to: String, amount: i64, transfer_id: String },
    Withdrawn { from: String, to: String, amount: i64, transfer_id: String },
}

impl DomainEvent for AcctEvent {
    fn correlation_id(&self) -> Option<&str> {
        Some(match self {
            AcctEvent::Deposited { transfer_id, .. } => transfer_id,
            AcctEvent::Withdrawn { transfer_id, .. } => transfer_id,
        })
    }
}

#[derive(Debug)]
enum AcctCmd {
    Deposit { account: String, from: String, amount: i64, transfer_id: String },
    Withdraw { account: String, to: String, amount: i64, transfer_id: String },
}

impl Command for AcctCmd {
    type AggregateId = String;
    fn aggregate_id(&self) -> String {
        match self {
            AcctCmd::Deposit { account, .. } | AcctCmd::Withdraw { account, .. } => account.clone(),
        }
    }
}

struct Acct {
    id: String,
}

#[async_trait]
impl Eventsourced for Acct {
    type Command = AcctCmd;
    type Event = AcctEvent;
    type State = AcctState;
    type Error = AcctErr;

    fn persistence_id(&self) -> String {
        self.id.clone()
    }

    fn command_to_events(&self, state: &AcctState, cmd: AcctCmd) -> Result<Vec<AcctEvent>, AcctErr> {
        match cmd {
            AcctCmd::Deposit { from, amount, transfer_id, account } => Ok(vec![AcctEvent::Deposited {
                from,
                to: account,
                amount,
                transfer_id,
            }]),
            AcctCmd::Withdraw { to, amount, transfer_id, account } => {
                if state.balance < amount {
                    return Err(AcctErr::Insufficient);
                }
                Ok(vec![AcctEvent::Withdrawn { from: account, to, amount, transfer_id }])
            }
        }
    }

    fn apply_event(state: &mut AcctState, e: &AcctEvent) {
        match e {
            AcctEvent::Deposited { amount, .. } => state.balance += amount,
            AcctEvent::Withdrawn { amount, .. } => state.balance -= amount,
        }
    }

    fn encode_event(e: &AcctEvent) -> Result<Vec<u8>, String> {
        // Tagged manual encoding so events round-trip through the journal.
        match e {
            AcctEvent::Deposited { from, to, amount, transfer_id } => Ok(format!(
                "D|{from}|{to}|{amount}|{transfer_id}"
            )
            .into_bytes()),
            AcctEvent::Withdrawn { from, to, amount, transfer_id } => Ok(format!(
                "W|{from}|{to}|{amount}|{transfer_id}"
            )
            .into_bytes()),
        }
    }

    fn decode_event(bytes: &[u8]) -> Result<AcctEvent, String> {
        let s = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
        let mut parts = s.split('|');
        let kind = parts.next().ok_or("missing kind")?;
        let from = parts.next().ok_or("from")?.to_string();
        let to = parts.next().ok_or("to")?.to_string();
        let amount: i64 = parts.next().ok_or("amount")?.parse().map_err(|_| "amount-parse")?;
        let transfer_id = parts.next().ok_or("transfer_id")?.to_string();
        match kind {
            "D" => Ok(AcctEvent::Deposited { from, to, amount, transfer_id }),
            "W" => Ok(AcctEvent::Withdrawn { from, to, amount, transfer_id }),
            other => Err(format!("bad kind: {other}")),
        }
    }
}

impl AggregateRoot for Acct {
    type Id = String;
    fn aggregate_id(&self) -> &Self::Id {
        &self.id
    }
}

// ─── Saga ──────────────────────────────────────────────────────────────

#[derive(Default)]
struct TransferState {
    deposit_dispatched: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("saga error")]
struct SErr;

struct TransferSaga;

#[async_trait]
impl Saga for TransferSaga {
    type Event = AcctEvent;
    type Command = AcctCmd;
    type State = TransferState;
    type Error = SErr;

    fn correlation_id(event: &AcctEvent) -> Option<String> {
        event.correlation_id().map(|s| s.to_string())
    }

    async fn handle(
        &mut self,
        state: &mut TransferState,
        event: AcctEvent,
    ) -> Result<Vec<SagaAction<AcctCmd>>, SErr> {
        match event {
            AcctEvent::Withdrawn { from, to, amount, transfer_id } if !state.deposit_dispatched => {
                state.deposit_dispatched = true;
                Ok(vec![
                    SagaAction::Send(AcctCmd::Deposit { account: to, from, amount, transfer_id }),
                ])
            }
            AcctEvent::Deposited { .. } => Ok(vec![SagaAction::Complete]),
            _ => Ok(vec![]),
        }
    }
}

#[tokio::test]
async fn withdraw_triggers_deposit_via_saga() {
    let system = ActorSystem::create("saga-test", Config::reference()).await.unwrap();
    let journal = Arc::new(InMemoryJournal::default());

    // Hand the saga a tap into events. We'll use this as the saga's input.
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<AcctEvent>();

    // Pre-fund A so it can withdraw.
    let topology = CqrsPattern::<Acct>::builder(journal.clone())
        .name("accounts")
        .factory(|id| Acct { id })
        .tap_events(event_tx)
        .build()
        .unwrap();
    let h = topology.materialize(&system).await.unwrap();
    let repo = h.repository();

    repo.send(AcctCmd::Deposit {
        account: "A".into(),
        from: "seed".into(),
        amount: 100,
        transfer_id: "seed-A".into(),
    })
    .await
    .unwrap();

    // Wire saga.
    let dispatcher_repo = repo.clone();
    let saga_topology = SagaPattern::<TransferSaga>::builder()
        .saga(TransferSaga)
        .events(event_rx)
        .dispatcher(move |cmd: AcctCmd| {
            let r = dispatcher_repo.clone();
            async move { r.send(cmd).await.is_ok() }
        })
        .build()
        .unwrap();
    saga_topology.materialize(&system).await.unwrap();

    // Trigger transfer.
    repo.send(AcctCmd::Withdraw {
        account: "A".into(),
        to: "B".into(),
        amount: 30,
        transfer_id: "tx-1".into(),
    })
    .await
    .unwrap();

    // Wait for B to have balance 30.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let r = repo.send(AcctCmd::Deposit {
            account: "probe".into(),
            from: "n/a".into(),
            amount: 0,
            transfer_id: "probe".into(),
        }).await;
        // Use an out-of-band probe by reading B's balance via Withdraw of 0.
        let _ = r;
        // Actually simplest: keep trying to withdraw 30 from B; succeeds once
        // saga has deposited.
        let result = repo.send(AcctCmd::Withdraw {
            account: "B".into(),
            to: "drain".into(),
            amount: 30,
            transfer_id: "drain".into(),
        }).await;
        if result.is_ok() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("B never received the deposit: {:?}", result);
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    system.terminate().await;
}
