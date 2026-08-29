use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::progress::ProgressSender;
use crate::store::Store;

#[derive(Clone, Deserialize)]
#[serde(tag = "type")]
enum Request {
    Install(InstallRequest),
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InstallRequest {
    id: String,
    home: PathBuf,
    plan: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeOutcome {
    Complete,
    ProtocolError,
}

#[derive(Default)]
struct State {
    active: HashSet<String>,
    protocol_failed: bool,
    fatal_diagnostic_pending: bool,
    output_failed: bool,
}

enum Event {
    Request(Request),
    ProtocolError {
        id: Option<String>,
        message: String,
    },
    InstallFinished {
        id: String,
        result: std::result::Result<String, String>,
    },
    TerminalWritten {
        id: String,
    },
    DiagnosticWritten {
        fatal: bool,
    },
    OutputFailed,
}

enum Effect {
    Install(InstallRequest),
    WriteTerminal { id: String, value: Value },
    WriteDiagnostic { value: Value, fatal: bool },
}

#[derive(Clone)]
struct EffectContext {
    store: Store,
    completion_output: mpsc::Sender<Value>,
    status_output: Option<mpsc::Sender<Value>>,
}

impl Event {
    fn reduce(self, state: &mut State, effects: &mut Vec<Effect>, events_to_stderr: bool) {
        match self {
            Self::Request(request) => {
                let Request::Install(request) = request;
                if request.id.is_empty() {
                    Self::protocol_error(
                        Some(request.id),
                        "request id must not be empty".into(),
                        state,
                        effects,
                        events_to_stderr,
                    );
                } else if !state.active.insert(request.id.clone()) {
                    Self::protocol_error(
                        Some(request.id),
                        "request id is already in flight".into(),
                        state,
                        effects,
                        events_to_stderr,
                    );
                } else {
                    effects.push(Effect::Install(request));
                }
            }
            Self::ProtocolError { id, message } => {
                Self::protocol_error(id, message, state, effects, events_to_stderr)
            }
            Self::InstallFinished { id, result } => {
                let value = match result {
                    Ok(root) => json!({"id": id, "type": "result", "root": root}),
                    Err(message) => json!({"id": id, "type": "error", "message": message}),
                };
                effects.push(Effect::WriteTerminal { id, value });
            }
            Self::TerminalWritten { id } => {
                state.active.remove(&id);
            }
            Self::DiagnosticWritten { fatal } => {
                if fatal {
                    state.fatal_diagnostic_pending = false;
                }
            }
            Self::OutputFailed => state.output_failed = true,
        }
    }

    fn protocol_error(
        id: Option<String>,
        message: String,
        state: &mut State,
        effects: &mut Vec<Effect>,
        events_to_stderr: bool,
    ) {
        let fatal = !events_to_stderr;
        if fatal {
            state.protocol_failed = true;
            state.fatal_diagnostic_pending = true;
        }
        effects.push(Effect::WriteDiagnostic {
            value: json!({"id": id, "type": "error", "message": message}),
            fatal,
        });
    }
}

impl Effect {
    async fn apply(self, ctx: EffectContext) -> Vec<Event> {
        match self {
            Self::Install(request) => {
                let result = match &ctx.status_output {
                    Some(output) => {
                        ctx.store
                            .install_with_progress(
                                &request.home,
                                request.plan.clone(),
                                ProgressSender::new(output.clone()),
                            )
                            .await
                    }
                    None => ctx.store.install(&request.home, request.plan.clone()).await,
                };
                vec![Event::InstallFinished {
                    id: request.id,
                    result: result
                        .map(|root| root.to_string_lossy().into_owned())
                        .map_err(|error| format!("{error:#}")),
                }]
            }
            Self::WriteTerminal { id, value } => {
                if ctx.completion_output.send(value).await.is_ok() {
                    vec![Event::TerminalWritten { id }]
                } else {
                    vec![Event::OutputFailed]
                }
            }
            Self::WriteDiagnostic { value, fatal } => {
                let output = ctx.status_output.as_ref().unwrap_or(&ctx.completion_output);
                if output.send(value).await.is_ok() {
                    vec![Event::DiagnosticWritten { fatal }]
                } else {
                    vec![Event::OutputFailed]
                }
            }
        }
    }
}

pub async fn serve(store: Store, events_to_stderr: bool) -> Result<ServeOutcome> {
    let (writer_stopped, mut writer_status) = watch::channel(false);
    let (completion_output, completion_events) = mpsc::channel::<Value>(128);
    let stdout_writer = spawn_writer(
        tokio::io::stdout(),
        completion_events,
        writer_stopped.clone(),
    );
    let (status_output, stderr_writer) = if events_to_stderr {
        let (send, receive) = mpsc::channel::<Value>(128);
        let writer = spawn_writer(tokio::io::stderr(), receive, writer_stopped.clone());
        (Some(send), Some(writer))
    } else {
        (None, None)
    };
    drop(writer_stopped);

    let ctx = EffectContext {
        store,
        completion_output: completion_output.clone(),
        status_output: status_output.clone(),
    };
    let (event_send, mut event_receive) = mpsc::channel(128);
    let mut effects = JoinSet::new();
    let mut state = State::default();
    let mut input = BufReader::new(tokio::io::stdin());
    let mut line = Vec::new();
    let mut input_closed = false;

    loop {
        if (input_closed && state.active.is_empty() && effects.is_empty())
            || state.output_failed
            || (state.protocol_failed && !state.fatal_diagnostic_pending)
        {
            break;
        }

        tokio::select! {
            read = input.read_until(b'\n', &mut line), if !input_closed && !state.protocol_failed => {
                let event = match read {
                    Ok(0) => {
                        input_closed = true;
                        None
                    }
                    Ok(_) if line.iter().all(u8::is_ascii_whitespace) => None,
                    Ok(_) => Some(match serde_json::from_slice::<Request>(&line) {
                        Ok(request) => Event::Request(request),
                        Err(error) => Event::ProtocolError {
                            id: recover_id(&line),
                            message: error.to_string(),
                        },
                    }),
                    Err(error) => {
                        input_closed = true;
                        Some(Event::ProtocolError {
                            id: None,
                            message: error.to_string(),
                        })
                    }
                };
                line.clear();
                if let Some(event) = event {
                    dispatch(event, &mut state, &mut effects, &ctx, &event_send, events_to_stderr);
                }
            }
            event = event_receive.recv() => {
                if let Some(event) = event {
                    dispatch(event, &mut state, &mut effects, &ctx, &event_send, events_to_stderr);
                }
            }
            result = effects.join_next(), if !effects.is_empty() => {
                if let Some(result) = result {
                    result.context("serve effect task failed")?;
                }
            }
            changed = writer_status.changed() => {
                if changed.is_ok() && *writer_status.borrow() {
                    state.output_failed = true;
                }
            }
        }
    }

    if state.output_failed || state.protocol_failed {
        effects.abort_all();
    }
    while let Some(result) = effects.join_next().await {
        if !result.as_ref().is_err_and(|error| error.is_cancelled()) {
            result.context("serve effect task failed")?;
        }
    }
    drop(event_send);
    drop(ctx);
    drop(completion_output);
    drop(status_output);
    stdout_writer.await.context("stdout writer task failed")??;
    if let Some(writer) = stderr_writer {
        writer.await.context("stderr writer task failed")??;
    }

    Ok(if state.protocol_failed {
        ServeOutcome::ProtocolError
    } else {
        ServeOutcome::Complete
    })
}

fn dispatch(
    event: Event,
    state: &mut State,
    tasks: &mut JoinSet<()>,
    ctx: &EffectContext,
    event_send: &mpsc::Sender<Event>,
    events_to_stderr: bool,
) {
    let mut effects = Vec::new();
    event.reduce(state, &mut effects, events_to_stderr);
    for effect in effects {
        let ctx = ctx.clone();
        let event_send = event_send.clone();
        tasks.spawn(async move {
            for event in effect.apply(ctx).await {
                if event_send.send(event).await.is_err() {
                    break;
                }
            }
        });
    }
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

fn recover_id(line: &[u8]) -> Option<String> {
    serde_json::from_slice::<Value>(line)
        .ok()?
        .get("id")?
        .as_str()
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reducer_releases_an_id_only_after_the_terminal_output_effect() {
        let mut state = State::default();
        let mut effects = Vec::new();
        Event::Request(Request::Install(InstallRequest {
            id: "request-1".into(),
            home: "/requests".into(),
            plan: json!({}),
        }))
        .reduce(&mut state, &mut effects, false);
        assert!(state.active.contains("request-1"));
        assert!(matches!(effects.as_slice(), [Effect::Install(_)]));

        effects.clear();
        Event::InstallFinished {
            id: "request-1".into(),
            result: Ok("/store/install/plan/root".into()),
        }
        .reduce(&mut state, &mut effects, false);
        assert!(state.active.contains("request-1"));
        assert!(matches!(effects.as_slice(), [Effect::WriteTerminal { .. }]));

        Event::TerminalWritten {
            id: "request-1".into(),
        }
        .reduce(&mut state, &mut Vec::new(), false);
        assert!(!state.active.contains("request-1"));
    }
}
