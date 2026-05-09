//! Specification pattern — composable predicates over a domain type.
//!
//! Useful for invariant checks, query filters, and command routing.
//! Specifications combine with `and`, `or`, and `not` so complex
//! predicates stay declarative.
//!
//! ```ignore
//! struct OverThreshold(i64);
//! impl Specification<Order> for OverThreshold {
//!     fn is_satisfied_by(&self, o: &Order) -> bool { o.amount > self.0 }
//! }
//! struct InRegion(String);
//! impl Specification<Order> for InRegion {
//!     fn is_satisfied_by(&self, o: &Order) -> bool { o.region == self.0 }
//! }
//! let spec = OverThreshold(100).and(InRegion("EU".into()));
//! orders.iter().filter(|o| spec.is_satisfied_by(o));
//! ```

/// Composable predicate over `T`.
pub trait Specification<T>: Send + Sync {
    fn is_satisfied_by(&self, t: &T) -> bool;

    fn and<S>(self, other: S) -> AndSpec<Self, S>
    where
        Self: Sized,
        S: Specification<T>,
    {
        AndSpec { a: self, b: other }
    }

    fn or<S>(self, other: S) -> OrSpec<Self, S>
    where
        Self: Sized,
        S: Specification<T>,
    {
        OrSpec { a: self, b: other }
    }

    fn not(self) -> NotSpec<Self>
    where
        Self: Sized,
    {
        NotSpec { inner: self }
    }
}

pub struct AndSpec<A, B> {
    a: A,
    b: B,
}
impl<T, A: Specification<T>, B: Specification<T>> Specification<T> for AndSpec<A, B> {
    fn is_satisfied_by(&self, t: &T) -> bool {
        self.a.is_satisfied_by(t) && self.b.is_satisfied_by(t)
    }
}

pub struct OrSpec<A, B> {
    a: A,
    b: B,
}
impl<T, A: Specification<T>, B: Specification<T>> Specification<T> for OrSpec<A, B> {
    fn is_satisfied_by(&self, t: &T) -> bool {
        self.a.is_satisfied_by(t) || self.b.is_satisfied_by(t)
    }
}

pub struct NotSpec<S> {
    inner: S,
}
impl<T, S: Specification<T>> Specification<T> for NotSpec<S> {
    fn is_satisfied_by(&self, t: &T) -> bool {
        !self.inner.is_satisfied_by(t)
    }
}

/// Convenience: lift any `Fn(&T) -> bool` into a [`Specification`].
pub struct FnSpec<F>(pub F);
impl<T, F: Fn(&T) -> bool + Send + Sync> Specification<T> for FnSpec<F> {
    fn is_satisfied_by(&self, t: &T) -> bool {
        (self.0)(t)
    }
}
