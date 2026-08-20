// Owns UI commands and delivery of runtime events.

use crate::{
    rpc::{Notice, Rpc, Stamped},
    sess::{Call, Sess, Status},
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc, thread};
use tauri::{AppHandle, Emitter, State};

const TURN_EVENT: &str = "turn-event";

#[derive(Serialize)]
struct StartRequest<'a> {
    #[serde(flatten)]
    call: &'a Call,
    query: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    transcript: &'a str,
}

#[derive(Deserialize)]
struct Accepted {
    #[serde(flatten)]
    call: Call,
    accepted: bool,
}

#[derive(Deserialize)]
struct Cancelled {
    cancelled: bool,
}

#[tauri::command]
pub async fn send_message(
    app: AppHandle,
    sess: State<'_, Arc<Sess>>,
    rpc: State<'_, Rpc>,
    text: String,
) -> std::result::Result<Call, String> {
    let sess = Arc::clone(sess.inner());
    let rpc = rpc.inner().clone();
    tauri::async_runtime::spawn_blocking(move || send(&app, &sess, &rpc, text))
        .await
        .map_err(|error| format!("send task failed: {error}"))?
        .map_err(message)
}

#[tauri::command]
pub async fn stop_turn(
    sess: State<'_, Arc<Sess>>,
    rpc: State<'_, Rpc>,
) -> std::result::Result<bool, String> {
    let sess = Arc::clone(sess.inner());
    let rpc = rpc.inner().clone();
    tauri::async_runtime::spawn_blocking(move || stop(&sess, &rpc))
        .await
        .map_err(|error| format!("stop task failed: {error}"))?
        .map_err(message)
}

pub fn listen(
    app: AppHandle,
    sess: Arc<Sess>,
    input: std::sync::mpsc::Receiver<Stamped>,
) -> Result<()> {
    thread::Builder::new()
        .name("turn-events".into())
        .spawn(move || {
            let mut next = 1;
            let mut pending = BTreeMap::new();
            while let Ok(stamped) = input.recv() {
                pending.insert(stamped.order, stamped.notice);
                while let Some(notice) = pending.remove(&next) {
                    deliver(&app, &sess, notice);
                    next += 1;
                }
            }
            if !pending.is_empty() {
                eprintln!("rpc closed with unordered turn notifications");
            }
        })
        .context("start turn event listener")?;
    Ok(())
}

fn send(app: &AppHandle, sess: &Sess, rpc: &Rpc, text: String) -> Result<Call> {
    let start = sess.open(text)?;
    if let Err(error) = app.emit(TURN_EVENT, &start.event) {
        sess.close(
            &start.call,
            Status::Failed,
            Some("opened turn could not reach the interface".into()),
        )?;
        return Err(error).context("emit opened turn");
    }
    let accepted: Accepted = match rpc.call(
        "turn.start",
        &StartRequest {
            call: &start.call,
            query: &start.query,
            transcript: &start.transcript,
        },
    ) {
        Ok(value) => value,
        Err(error) => {
            cancel_quietly(rpc, &start.call);
            close_failed(app, sess, &start.call, error.to_string())?;
            return Err(error).context("start model turn");
        }
    };
    if !accepted.accepted || accepted.call != start.call {
        let detail = "model service returned a mismatched turn";
        cancel_quietly(rpc, &start.call);
        close_failed(app, sess, &start.call, detail.into())?;
        anyhow::bail!(detail);
    }
    if sess.started(&start.call)? {
        let _: Cancelled = rpc
            .call("turn.cancel", &start.call)
            .context("finish queued cancellation")?;
    }
    Ok(start.call)
}

fn stop(sess: &Sess, rpc: &Rpc) -> Result<bool> {
    let Some(call) = sess.request_cancel()? else {
        return Ok(false);
    };
    let result: Cancelled = rpc
        .call("turn.cancel", &call)
        .context("cancel model turn")?;
    Ok(result.cancelled)
}

fn cancel_quietly(rpc: &Rpc, call: &Call) {
    if let Err(error) = rpc.call::<_, Cancelled>("turn.cancel", call) {
        eprintln!("cancel unconfirmed turn: {error:#}");
    }
}

fn close_failed(app: &AppHandle, sess: &Sess, call: &Call, error: String) -> Result<()> {
    if let Some(event) = sess.close(call, Status::Failed, Some(error))? {
        app.emit(TURN_EVENT, event).context("emit failed turn")?;
    }
    Ok(())
}

fn deliver(app: &AppHandle, sess: &Sess, notice: std::result::Result<Notice, String>) {
    let event = match notice {
        Ok(Notice::Chunk { call, text }) => sess.chunk(&call, text),
        Ok(Notice::Closed {
            call,
            status,
            error,
        }) => sess.close(&call, status, error),
        Err(error) => {
            eprintln!("invalid turn notification: {error}");
            return;
        }
    };
    match event {
        Ok(Some(event)) => {
            if let Err(error) = app.emit(TURN_EVENT, event) {
                eprintln!("emit turn event: {error}");
            }
        }
        Ok(None) => {}
        Err(error) => eprintln!("record turn notification: {error:#}"),
    }
}

fn message(error: impl std::fmt::Display) -> String {
    error.to_string()
}
