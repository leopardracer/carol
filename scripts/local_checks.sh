#!/bin/bash

# Run all checks locally

cargo fmt --check
cargo clippy --locked --no-deps --all-targets --all-features -- --deny warnings
RUSTFLAGS="-D warnings" cargo check --all-targets --all-features --locked
cargo test --all-targets --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
