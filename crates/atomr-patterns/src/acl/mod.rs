//! Anti-Corruption Layer pattern.
//!
//! Translates events / commands between two bounded contexts. The user
//! provides a [`Translator`] mapping `External -> Option<Internal>`
//! (returning `None` drops the message).
//!
//! v1 implementation: a tokio task that reads from an
//! [`tokio::sync::mpsc::UnboundedReceiver<External>`] input, applies
//! the translator, and pushes survivors to an output
//! [`tokio::sync::mpsc::UnboundedSender<Internal>`].

use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use atomr_core::actor::ActorSystem;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::topology::Topology;
use crate::PatternError;

/// Translate a value from one bounded context's vocabulary to
/// another's. Return `None` to drop the value.
pub trait Translator: Send + Sync + 'static {
    type External: Send + 'static;
    type Internal: Send + 'static;
    fn translate(&self, ext: Self::External) -> Option<Self::Internal>;
}

/// Public handle to the ACL pattern.
pub struct AntiCorruption<X, I>(PhantomData<(X, I)>);

impl<X: Send + 'static, I: Send + 'static> AntiCorruption<X, I> {
    pub fn builder<T>(translator: T) -> AclBuilder<T>
    where
        T: Translator<External = X, Internal = I>,
    {
        AclBuilder { name: None, translator: Arc::new(translator) }
    }
}

pub struct AclBuilder<T: Translator> {
    name: Option<String>,
    translator: Arc<T>,
}

impl<T: Translator> AclBuilder<T> {
    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = Some(n.into());
        self
    }

    pub fn build(self) -> AclTopology<T> {
        AclTopology {
            name: self.name.unwrap_or_else(|| "acl".into()),
            translator: self.translator,
        }
    }
}

pub struct AclTopology<T: Translator> {
    #[allow(dead_code)]
    name: String,
    translator: Arc<T>,
}

/// Handles handed back after [`Topology::materialize`].
pub struct AclHandles<X, I> {
    pub input: UnboundedSender<X>,
    pub output: UnboundedReceiver<I>,
}

#[async_trait]
impl<T: Translator> Topology for AclTopology<T> {
    type Handles = AclHandles<T::External, T::Internal>;

    async fn materialize(self, _system: &ActorSystem) -> Result<Self::Handles, PatternError<()>> {
        let (in_tx, mut in_rx) = unbounded_channel::<T::External>();
        let (out_tx, out_rx) = unbounded_channel::<T::Internal>();
        let translator = self.translator.clone();
        tokio::spawn(async move {
            while let Some(ext) = in_rx.recv().await {
                if let Some(int) = translator.translate(ext) {
                    if out_tx.send(int).is_err() {
                        break;
                    }
                }
            }
        });
        Ok(AclHandles { input: in_tx, output: out_rx })
    }
}
