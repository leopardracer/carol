use diesel::result::DatabaseErrorKind;

#[doc(no_inline)]
pub use diesel::result::{ConnectionError, Error as DieselError};

#[doc(no_inline)]
pub use diesel_async::pooled_connection::deadpool::{BuildError, PoolError};

use crate::database::StorageDatabaseError;
use crate::error::NonUtf8PathError;

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

    #[error(transparent)]
    StorePolicyError(#[from] ConvertStorePolicyError),

    #[error(transparent)]
    CreateNewFileError(#[from] CreateNewFileError),
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

/// Error during serializing or deserializing store policy.
#[derive(thiserror::Error, Debug)]
pub enum ConvertStorePolicyError {
    /// Failed to convert store policy enum variant.
    #[error("failed to convert store policy enum variant")]
    TryFromIntError(#[from] std::num::TryFromIntError),

    /// Store policy data is missing.
    #[error("store policy data is missing")]
    MissingPolicyData,

    #[error("failed to deserialize enum variant: {0}")]
    BadEnumVariat(String),
}

impl ConvertStorePolicyError {
    pub fn bad_enum_variant(msg: String) -> Self {
        Self::BadEnumVariat(msg)
    }
}

/// Error during creation of new file in cache database.
#[derive(thiserror::Error, Debug)]
pub enum CreateNewFileError {
    /// Failed to serialize or deserialize store policy.
    #[error(transparent)]
    StorePolicyError(#[from] ConvertStorePolicyError),

    /// Failed to convert paths.
    #[error(transparent)]
    NonUtf8PathError(#[from] NonUtf8PathError),
}
