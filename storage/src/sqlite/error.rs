use diesel::result::DatabaseErrorKind;

#[doc(no_inline)]
pub use diesel::result::{ConnectionError, Error as DieselError};

#[doc(no_inline)]
pub use diesel_async::pooled_connection::deadpool::{BuildError, PoolError};

use crate::database::StorageDatabaseError;

/// Cache database related errors.
#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("connection failed")]
    ConnectionError(#[from] ConnectionError),

    #[error("failed to build connection pool")]
    PoolBuildError(#[from] BuildError),

    #[error("connection pool error")]
    PoolError(#[from] PoolError),

    #[error("migration failed: {0}")]
    MigrationError(String),

    #[error(transparent)]
    DieselError(#[from] DieselError),

    #[error("failed to remove entry")]
    RemoveError(#[from] RemoveErrorReason),
}

impl StorageDatabaseError for DatabaseError {
    fn is_unique_violation(&self) -> bool {
        matches!(
            *self,
            DatabaseError::DieselError(DieselError::DatabaseError(
                DatabaseErrorKind::UniqueViolation,
                _
            ))
        )
    }

    fn is_not_found(&self) -> bool {
        matches!(*self, DatabaseError::DieselError(DieselError::NotFound))
    }
}

/// Reason why remove failed.
#[derive(thiserror::Error, Debug)]
pub enum RemoveErrorReason {
    /// Reference counter is not 0
    #[error("reference counter is not 0")]
    UsedFile,

    /// Entry status is not 'ToRemove'
    #[error("entry status is not 'ToRemove'")]
    WrongStatus,
}
