/// Convergent replicated data type merge semantics.
///
/// Mirrors Akka.DistributedData's `IReplicatedData.Merge`.
pub trait CrdtMerge: Clone {
    fn merge(&mut self, other: &Self);
}
