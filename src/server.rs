use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::progress::ProgressSender;
use crate::store::Store;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstallRequest {
    id: String,
    plan_file: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeOutcome {
    Complete,
    ProtocolError,
}

pub async fn serve(store: Store, events_to_stderr: bool) -> Result<ServeOutcome> {
    let (writer_stopped, mut writer_status) = watch::channel(false);
    let (completion_output, completion_events) = mpsc::channel::<Value>(128);
    let stdout_writer = spawn_writer(
        tokio::io::stdout(),
        completion_events,
        writer_stopped.clone(),
    );
    let (event_output, stderr_writer) = if events_to_stderr {
        let (send, receive) = mpsc::channel::<Value>(128);
        let writer = spawn_writer(tokio::io::stderr(), receive, writer_stopped.clone());
        (Some(send), Some(writer))
    } else {
        (None, None)
    };
    drop(writer_stopped);

    let active = Arc::new(Mutex::new(HashSet::<String>::new()));
    let mut tasks = JoinSet::new();
    let mut input = BufReader::new(tokio::io::stdin());
    let mut line = Vec::new();
    let mut output_failed = false;
    let mut protocol_failed = false;
    loop {
        line.clear();
        let read_result = tokio::select! {
            result = input.read_until(b'\n', &mut line) => result,
            result = writer_status.changed() => {
                if result.is_ok() && *writer_status.borrow() {
                    output_failed = true;
                    break;
                }
                continue;
            }
        };
        let count = match read_result {
            Ok(count) => count,
            Err(error) => {
                let _ = send_diagnostic(
                    &completion_output,
                    event_output.as_ref(),
                    json!({"id": null, "type": "error", "message": error.to_string()}),
                )
                .await;
                protocol_failed = true;
                break;
            }
        };
        if count == 0 {
            break;
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }

        let request = match serde_json::from_slice::<InstallRequest>(&line) {
            Ok(request) => request,
            Err(error) => {
                let id = recover_id(&line);
                if !send_diagnostic(
                    &completion_output,
                    event_output.as_ref(),
                    json!({"id": id, "type": "error", "message": error.to_string()}),
                )
                .await
                {
                    output_failed = true;
                    break;
                }
                if events_to_stderr {
                    continue;
                }
                protocol_failed = true;
                break;
            }
        };
        if request.id.is_empty() {
            if !send_diagnostic(
                &completion_output,
                event_output.as_ref(),
                json!({
                    "id": request.id,
                    "type": "error",
                    "message": "request id must not be empty"
                }),
            )
            .await
            {
                output_failed = true;
                break;
            }
            if events_to_stderr {
                continue;
            }
            protocol_failed = true;
            break;
        }
        {
            let mut active_ids = active.lock().await;
            if !active_ids.insert(request.id.clone()) {
                drop(active_ids);
                if !send_diagnostic(
                    &completion_output,
                    event_output.as_ref(),
                    json!({
                        "id": request.id,
                        "type": "error",
                        "message": "request id is already in flight"
                    }),
                )
                .await
                {
                    output_failed = true;
                    break;
                }
                if events_to_stderr {
                    continue;
                }
                protocol_failed = true;
                break;
            }
        }

        let store = store.clone();
        let completion_output = completion_output.clone();
        let progress_output = event_output.clone();
        let active = active.clone();
        tasks.spawn(async move {
            let id = request.id;
            let result = if let Some(progress_output) = progress_output {
                let (progress_send, mut progress_receive) = mpsc::channel(64);
                let progress_id = id.clone();
                let bridge = tokio::spawn(async move {
                    while let Some(event) = progress_receive.recv().await {
                        if progress_output
                            .send(progress_event(&progress_id, event))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                });
                let result = store
                    .create_with_progress(&request.plan_file, ProgressSender::new(progress_send))
                    .await;
                bridge.await.context("progress bridge failed")?;
                result
            } else {
                store.create(&request.plan_file).await
            };
            let terminal = match result {
                Ok(root) => json!({
                    "id": id,
                    "type": "result",
                    "root": root.to_string_lossy()
                }),
                Err(error) => json!({
                    "id": id,
                    "type": "error",
                    "message": format!("{error:#}")
                }),
            };
            let _ = completion_output.send(terminal).await;
            active.lock().await.remove(&id);
            Result::<()>::Ok(())
        });
    }

    if output_failed || protocol_failed {
        tasks.abort_all();
    }
    while let Some(result) = tasks.join_next().await {
        if !result
            .as_ref()
            .is_err_and(tokio::task::JoinError::is_cancelled)
        {
            result.context("install request task failed")??;
        }
    }
    drop(completion_output);
    drop(event_output);
    stdout_writer.await.context("stdout writer task failed")??;
    if let Some(writer) = stderr_writer {
        writer.await.context("stderr writer task failed")??;
    }
    Ok(if protocol_failed {
        ServeOutcome::ProtocolError
    } else {
        ServeOutcome::Complete
    })
}

fn spawn_writer<W>(
    output: W,
    mut events: mpsc::Receiver<Value>,
    writer_stopped: watch::Sender<bool>,
) -> JoinHandle<Result<()>>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut output = BufWriter::new(output);
        let result = async {
            while let Some(event) = events.recv().await {
                let mut line = serde_json::to_vec(&event)?;
                line.push(b'\n');
                output.write_all(&line).await?;
                output.flush().await?;
            }
            Result::<()>::Ok(())
        }
        .await;
        let _ = writer_stopped.send(true);
        result
    })
}

async fn send_diagnostic(
    completion_output: &mpsc::Sender<Value>,
    event_output: Option<&mpsc::Sender<Value>>,
    event: Value,
) -> bool {
    match event_output {
        Some(output) => output.send(event).await.is_ok(),
        None => completion_output.send(event).await.is_ok(),
    }
}

fn recover_id(line: &[u8]) -> Option<String> {
    serde_json::from_slice::<Value>(line)
        .ok()?
        .get("id")?
        .as_str()
        .map(str::to_owned)
}

fn progress_event(id: &str, event: Value) -> Value {
    let mut output = Map::new();
    output.insert("id".into(), Value::String(id.to_owned()));
    output.insert("type".into(), Value::String("progress".into()));
    let mut event = event.as_object().cloned().unwrap_or_default();
    let event_name = event
        .remove("event")
        .and_then(|value| value.as_str().map(str::to_owned));
    let stage = if event_name.as_deref() == Some("waiting") {
        "waiting"
    } else if event.contains_key("dl_key") {
        "download"
    } else {
        "install"
    };
    output.insert("stage".into(), Value::String(stage.into()));
    output.extend(event);
    Value::Object(output)
}
