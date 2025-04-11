use clap::Parser;

#[cfg(not(tarpaulin_include))]
mod cli;

#[tokio::main]
#[cfg(not(tarpaulin_include))]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .init();
    cli::Cli::parse().execute().await
}
