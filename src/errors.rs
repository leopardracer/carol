//! Error types.

use diesel::result::DatabaseErrorKind;

#[doc(no_inline)]
pub use diesel::result::{ConnectionError, Error as DieselError};

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("database connection failed: {0}")]
    ConnectionError(ConnectionError),

    #[error("database migration failed: {0}")]
    MigrationError(String),

    #[error("database error: {0}")]
    DieselError(#[from] DieselError),

    #[error("failed to remove entry: {0}")]
    RemoveError(RemoveError),
}

#[derive(thiserror::Error, Debug)]
pub enum RemoveError {
    #[error("reference counter is not 0")]
    UsedFile,

    #[error("entry status is not ToRemove")]
    WrongStatus,
}

impl DatabaseError {
    /// Whether the error is unique key violation.
    pub fn is_unique_violation(&self) -> bool {
        matches!(
            *self,
            DatabaseError::DieselError(DieselError::DatabaseError(
                DatabaseErrorKind::UniqueViolation,
                _
            ))
        )
    }
}

/// Carol error.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    DatabaseError(#[from] DatabaseError),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("download error: {0}")]
    DownloadError(#[from] reqwest::Error),

    #[error("awaited file failed to download")]
    AwaitingError,

    #[error("{0}")]
    CustomError(String),

    #[error("internal error: {0}")]
    InternalError(String),

    #[error("{0}")]
    NonUtf8PathError(#[from] NonUtf8PathError),
}

/// Non UTF-8 symbol in path.
#[derive(thiserror::Error, Debug)]
#[error("non-UTF-8 symbol in path")]
pub struct NonUtf8PathError;

/// Maintenance operations error.
#[derive(thiserror::Error, Debug)]
pub enum MaintenanceError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("client error: {0}")]
    ClientError(#[from] Error),

    #[error("{0}")]
    NonUtf8PathError(#[from] NonUtf8PathError),
}
