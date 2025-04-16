diesel::table! {
    /// Cached files metadata.
    files (id) {
        /// Primary key.
        id -> Integer,

        /// URL of the downloaded file or some other source.
        source -> VarChar,

        // Manually added UNIQUE to up.sql, because diesel can't do that
        /// Path to downloaded file in cache.
        cache_path -> VarChar,

        /// Original file name.
        filename -> Nullable<VarChar>,

        /// Entry creation timestamp.
        created -> TimestamptzSqlite,

        /// When the file was used last time.
        last_used -> TimestamptzSqlite,

        /// Store policy.
        store_policy -> Integer,

        /// Additional store policy data.
        store_policy_data -> Nullable<Integer>,

        /// Current status of the cache entry.
        status -> Integer,
    }
}
