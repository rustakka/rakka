//! Domain-Driven-Design primitives shared by every pattern.
//!
//! These traits are the *vocabulary*: an [`AggregateRoot`] is the
//! transactional consistency boundary, a [`Command`] requests a state
//! change inside one, a [`DomainEvent`] records the fact that the change
//! happened, an [`Entity`] is anything with a stable identity, and a
//! [`ValueObject`] is anything whose identity is its value. The
//! [`Repository`] is how callers reach an aggregate.

mod aggregate;
mod command;
mod domain_event;
mod entity;
mod repository;
mod value_object;

pub use aggregate::AggregateRoot;
pub use command::Command;
pub use domain_event::DomainEvent;
pub use entity::Entity;
pub use repository::Repository;
pub use value_object::ValueObject;
