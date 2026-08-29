use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use directories::ProjectDirs;
use imos::store::Store;

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
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let store_path = match cli.store {
        Some(path) => path,
        None => ProjectDirs::from("dev", "imos", "imos")
            .context("could not determine the user cache directory; pass --store explicitly")?
            .cache_dir()
            .to_path_buf(),
    };
    let store = Store::open(store_path)?;

    match cli.command {
        Command::Create { plan_file } => {
            let result = store.create(&plan_file)?;
            println!("{}", result.display());
        }
        Command::Remove { plan_file } => store.remove(&plan_file)?,
        Command::Gc => {
            let report = store.gc()?;
            println!(
                "install={} dl={} request={} tmp={}",
                report.installs, report.downloads, report.requests, report.temporary
            );
        }
    }

    Ok(())
}
