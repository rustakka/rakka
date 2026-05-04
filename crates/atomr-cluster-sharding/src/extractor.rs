//! `IMessageExtractor` equivalent from akka.net.

pub trait MessageExtractor: Send + Sync + 'static {
    type Message: Send + 'static;

    fn entity_id(&self, message: &Self::Message) -> String;
    fn shard_id(&self, message: &Self::Message) -> String;
}
