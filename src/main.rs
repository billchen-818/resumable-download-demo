mod download;
mod state;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use reqwest::Url;

#[derive(Parser)]
#[command(about = "Download an HTTP file and resume interrupted transfers")]
struct Args {
    url: Url,
    #[arg(short, long)]
    output: PathBuf,
    #[arg(long, value_name = "HEX")]
    sha256: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let expected_hash = args
        .sha256
        .map(|value| {
            let mut hash = [0; 32];
            hex::decode_to_slice(value, &mut hash).context("--sha256 needs 64 hex digits")?;
            Ok::<_, anyhow::Error>(hash)
        })
        .transpose()?;

    let bytes = download::run(&args.url, &args.output, expected_hash).await?;
    println!("Saved {} ({bytes} bytes)", args.output.display());
    Ok(())
}
