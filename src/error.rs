//! Error taxonomy.
//!
//! M4 in the roadmap requires that no panic be reachable from the public API.
//! Starting the taxonomy in M0 keeps that achievable: every fallible operation
//! returns `Result` from the outset rather than being retrofitted later.

use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Insert of a primary key that already exists (rendered key).
    DuplicateKey(String),
    /// Update or delete of a primary key that is not live (rendered key).
    KeyNotFound(String),
    /// A row does not match its table's schema (arity or type).
    SchemaMismatch(String),
    /// Two writers raced for the same row; the loser must retry.
    WriteConflict,
    /// Named table does not exist.
    TableNotFound(String),
    /// Attempt to create a table whose name is taken.
    TableExists(String),
    /// A SQL parse, plan, or type error.
    Sql(String),
    /// A row violated a declared constraint (NOT NULL, CHECK).
    ConstraintViolation(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::DuplicateKey(pk) => write!(f, "duplicate primary key: {pk}"),
            Error::KeyNotFound(pk) => write!(f, "primary key not found: {pk}"),
            Error::SchemaMismatch(msg) => write!(f, "schema mismatch: {msg}"),
            Error::WriteConflict => write!(f, "write conflict; retry the transaction"),
            Error::TableNotFound(name) => write!(f, "table not found: {name}"),
            Error::TableExists(name) => write!(f, "table already exists: {name}"),
            Error::Sql(msg) => write!(f, "sql error: {msg}"),
            Error::ConstraintViolation(msg) => write!(f, "constraint violation: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// True if the caller can reasonably retry the same operation.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::WriteConflict)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_include_context() {
        assert!(Error::DuplicateKey("42".into()).to_string().contains("42"));
        assert!(Error::KeyNotFound("7".into()).to_string().contains('7'));
        assert!(Error::TableNotFound("users".into())
            .to_string()
            .contains("users"));
        assert!(Error::TableExists("orders".into())
            .to_string()
            .contains("orders"));
        assert!(Error::WriteConflict.to_string().contains("retry"));
    }

    #[test]
    fn only_conflicts_are_retryable() {
        assert!(Error::WriteConflict.is_retryable());
        assert!(!Error::DuplicateKey("1".into()).is_retryable());
        assert!(!Error::KeyNotFound("1".into()).is_retryable());
        assert!(!Error::TableNotFound("x".into()).is_retryable());
    }

    #[test]
    fn implements_std_error() {
        let e: Box<dyn std::error::Error> = Box::new(Error::WriteConflict);
        assert!(!e.to_string().is_empty());
    }

    #[test]
    fn equality_works_for_assertions() {
        assert_eq!(
            Error::KeyNotFound("5".into()),
            Error::KeyNotFound("5".into())
        );
        assert_ne!(
            Error::KeyNotFound("5".into()),
            Error::KeyNotFound("6".into())
        );
    }
}
