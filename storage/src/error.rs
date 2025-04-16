/// Non UTF-8 symbol in path.
#[derive(thiserror::Error, Debug)]
#[error("non-UTF-8 symbol in path")]
pub struct NonUtf8PathError;
