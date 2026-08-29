use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};

pub struct IntentDb {
    connection: Connection,
}

impl IntentDb {
    pub fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path)
            .with_context(|| format!("open intent database {}", path.display()))?;
        connection.busy_timeout(std::time::Duration::from_secs(30))?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             PRAGMA foreign_keys = ON;
             CREATE TABLE IF NOT EXISTS request_plans (
                 request_ino TEXT PRIMARY KEY,
                 plan_key TEXT NOT NULL
             ) STRICT;
             CREATE INDEX IF NOT EXISTS request_plans_by_plan
                 ON request_plans(plan_key);
             CREATE TABLE IF NOT EXISTS plan_downloads (
                 plan_key TEXT NOT NULL,
                 dl_key TEXT NOT NULL,
                 PRIMARY KEY (plan_key, dl_key)
             ) STRICT, WITHOUT ROWID;",
        )?;
        Ok(Self { connection })
    }

    pub fn request_plan(&self, request_ino: &str) -> Result<Option<String>> {
        self.connection
            .query_row(
                "SELECT plan_key FROM request_plans WHERE request_ino = ?1",
                [request_ino],
                |row| row.get(0),
            )
            .optional()
            .context("query request intent")
    }

    pub fn add_request(
        &mut self,
        request_ino: &str,
        plan_key: &str,
        download_keys: &HashSet<String>,
    ) -> Result<()> {
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT INTO request_plans(request_ino, plan_key) VALUES (?1, ?2)
             ON CONFLICT(request_ino) DO UPDATE SET plan_key = excluded.plan_key",
            params![request_ino, plan_key],
        )?;
        for key in download_keys {
            transaction.execute(
                "INSERT OR IGNORE INTO plan_downloads(plan_key, dl_key) VALUES (?1, ?2)",
                params![plan_key, key],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn remove_request(&mut self, request_ino: &str) -> Result<Option<String>> {
        let transaction = self.connection.transaction()?;
        let plan_key = transaction
            .query_row(
                "SELECT plan_key FROM request_plans WHERE request_ino = ?1",
                [request_ino],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        transaction.execute(
            "DELETE FROM request_plans WHERE request_ino = ?1",
            [request_ino],
        )?;
        transaction.commit()?;
        Ok(plan_key)
    }

    pub fn live_plan_keys(&self) -> Result<HashSet<String>> {
        query_set(
            &self.connection,
            "SELECT DISTINCT plan_key FROM request_plans",
        )
    }

    pub fn live_download_keys(&self) -> Result<HashSet<String>> {
        query_set(
            &self.connection,
            "SELECT DISTINCT pd.dl_key
             FROM plan_downloads AS pd
             JOIN request_plans AS rp USING (plan_key)",
        )
    }

    pub fn request_inodes(&self) -> Result<HashSet<String>> {
        query_set(&self.connection, "SELECT request_ino FROM request_plans")
    }

    pub fn remove_unreferenced_download_relations(&self) -> Result<usize> {
        self.connection
            .execute(
                "DELETE FROM plan_downloads
                 WHERE NOT EXISTS (
                     SELECT 1 FROM request_plans
                     WHERE request_plans.plan_key = plan_downloads.plan_key
                 )",
                [],
            )
            .context("remove unreferenced download relations")
    }
}

fn query_set(connection: &Connection, sql: &str) -> Result<HashSet<String>> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    rows.collect::<rusqlite::Result<HashSet<_>>>()
        .context("read intent set")
}
