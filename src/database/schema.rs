diesel::table! {
    /// Cached files metadata.
    files (id) {
        /// Primary key.
        id -> Integer,

        // Manually added UNIQUE to up.sql, because diesel can't do that
        /// URL of the downloaded file.
        url -> VarChar,

        // Manually added UNIQUE to up.sql, because diesel can't do that
        /// Path to downloaded file in cache.
        cache_path -> VarChar,

        /// Entry creation timestamp.
        created -> TimestamptzSqlite,

        /// When the file was used last time.
        last_used -> TimestamptzSqlite,

        /// Cache policy.
        cache_policy -> VarChar,

        /// Current status of the cache entry.
        status -> Integer,

        /// Reference counter of the cache entry.
        ref_count -> Integer,
    }
}
