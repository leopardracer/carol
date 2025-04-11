use std::fmt::Debug;

pub trait StorageDatabase {
    /// Type of the URI used to address the database.
    type Uri: Clone + Debug + PartialEq + Eq;
}
