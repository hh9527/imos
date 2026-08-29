use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use directories::ProjectDirs;
use imos::progress::ProgressSender;
use imos::server::serve;
use imos::store::Store;
use serde_json::Value;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Deterministic artifact download, installation, and collection"
)]
struct Cli {
    /// Path to the IMOS store
    #[arg(long, global = true, env = "IMOS_STORE")]
    store: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Submit an immutable plan and create its installation
    Create { plan_file: PathBuf },
    /// Remove the intent represented by a plan file
    Remove { plan_file: PathBuf },
    /// Collect objects no longer needed by any plan file
    Gc,
    /// Serve install requests over stdin and stdout using JSON Lines
    Serv,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let store_path = match cli.store {
        Some(path) => path,
        None => ProjectDirs::from("dev", "imos", "imos")
            .context("could not determine the user cache directory; pass --store explicitly")?
            .cache_dir()
            .to_path_buf(),
    };
    let store = Store::open(store_path).await?;

    match cli.command {
        Command::Create { plan_file } => {
            let (send, receive) = mpsc::channel(64);
            let writer = tokio::spawn(write_progress(receive));
            let result = store
                .create_with_progress(&plan_file, ProgressSender::new(send))
                .await;
            writer.await.context("progress writer task failed")??;
            println!("{}", result?.display());
        }
        Command::Remove { plan_file } => store.remove(&plan_file).await?,
        Command::Gc => {
            let report = store.gc().await?;
            println!(
                "install={} dl={} request={} tmp={}",
                report.installs, report.downloads, report.requests, report.temporary
            );
        }
        Command::Serv => serve(store).await?,
    }

    Ok(())
}

async fn write_progress(mut receive: mpsc::Receiver<Value>) -> Result<()> {
    let mut stderr = BufWriter::new(tokio::io::stderr());
    while let Some(event) = receive.recv().await {
        let mut line = serde_json::to_vec(&event)?;
        line.push(b'\n');
        stderr.write_all(&line).await?;
        stderr.flush().await?;
    }
    Ok(())
}
