//! Error types.

use diesel::result::DatabaseErrorKind;

#[doc(no_inline)]
pub use diesel::result::{ConnectionError, Error as DieselError};

/// Cache database related errors.
#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("connection failed")]
    ConnectionError(#[from] ConnectionError),

    #[error("migration failed: {0}")]
    MigrationError(String),

    #[error(transparent)]
    DieselError(#[from] DieselError),

    #[error("failed to remove entry")]
    RemoveError(#[from] RemoveErrorReason),
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

/// Carol client error.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Cache database releated error
    #[error("database error")]
    DatabaseError(#[from] DatabaseError),

    /// Some I/O operation failed
    #[error("I/O error")]
    IoError(#[from] std::io::Error),

    /// Error during downloading of the file
    #[error("download error")]
    DownloadError(#[from] reqwest::Error),

    /// Failed to wait for file to be downloaded
    ///
    /// This happens when one thread is waiting for
    /// a file, which is currently being downloaded by another thread,
    /// and that download fails.
    #[error("awaited file failed to download")]
    AwaitingError,

    /// Custom error message
    #[error("{0}")]
    CustomError(String),

    #[error(transparent)]
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
