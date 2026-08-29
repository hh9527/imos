use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use directories::ProjectDirs;
use imos::progress::ProgressSender;
use imos::server::{ServeOutcome, serve};
use imos::store::Store;
use serde_json::Value;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
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
    Serv {
        /// Write progress and recoverable protocol errors to stderr
        #[arg(short = 'e', long)]
        events_to_stderr: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let server_mode = match &cli.command {
        Command::Serv { events_to_stderr } => Some(*events_to_stderr),
        _ => None,
    };
    match run(cli).await {
        Ok(true) => std::process::exit(1),
        Ok(false) => {}
        Err(error) => {
            if let Some(events_to_stderr) = server_mode {
                let _ = write_server_error(&error, events_to_stderr).await;
            } else {
                eprintln!("error: {error:#}");
            }
            std::process::exit(1);
        }
    }
}

async fn run(cli: Cli) -> Result<bool> {
    let store_path = match cli.store {
        Some(path) => path,
        None => ProjectDirs::from("dev", "imos", "imos")
            .context("could not determine the user cache directory; pass --store explicitly")?
            .cache_dir()
            .to_path_buf(),
    };
    let store = Store::open(store_path).await?;

    let protocol_failed = match cli.command {
        Command::Create { plan_file } => {
            let (send, receive) = mpsc::channel(64);
            let writer = tokio::spawn(write_progress(receive));
            let result = store
                .create_with_progress(&plan_file, ProgressSender::new(send))
                .await;
            writer.await.context("progress writer task failed")??;
            println!("{}", result?.display());
            false
        }
        Command::Remove { plan_file } => {
            store.remove(&plan_file).await?;
            false
        }
        Command::Gc => {
            let report = store.gc().await?;
            println!(
                "install={} dl={} request={} tmp={}",
                report.installs, report.downloads, report.requests, report.temporary
            );
            false
        }
        Command::Serv { events_to_stderr } => {
            serve(store, events_to_stderr).await? == ServeOutcome::ProtocolError
        }
    };

    Ok(protocol_failed)
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

async fn write_server_error(error: &anyhow::Error, events_to_stderr: bool) -> Result<()> {
    let event = serde_json::json!({
        "id": null,
        "type": "error",
        "message": format!("{error:#}")
    });
    if events_to_stderr {
        write_event(tokio::io::stderr(), &event).await
    } else {
        write_event(tokio::io::stdout(), &event).await
    }
}

async fn write_event<W>(output: W, event: &Value) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut line = serde_json::to_vec(event)?;
    line.push(b'\n');
    let mut output = BufWriter::new(output);
    output.write_all(&line).await?;
    output.flush().await?;
    Ok(())
}
