// Copyright © SixtyFPS GmbH <info@slint.dev>
// SPDX-License-Identifier: MIT

use crate::state::{HaEntity, SharedState, StateChange};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

type Error = Box<dyn std::error::Error + Send + Sync>;
type HaWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn send_json(ws: &mut HaWebSocket, value: Value) -> Result<(), Error> {
    ws.send(Message::Text(value.to_string().into())).await?;
    Ok(())
}

async fn recv_json(ws: &mut HaWebSocket) -> Result<Value, Error> {
    loop {
        let message = ws.next().await.ok_or("Home Assistant closed the WebSocket")??;
        if let Message::Text(text) = message {
            return Ok(serde_json::from_str(text.as_str())?);
        }
        if matches!(message, Message::Close(_)) {
            return Err("Home Assistant closed the WebSocket".into());
        }
    }
}

async fn wait_result(ws: &mut HaWebSocket, id: u64) -> Result<Value, Error> {
    loop {
        let message = recv_json(ws).await?;
        if message["id"] != id || message["type"] != "result" {
            continue;
        }
        if message["success"] != true {
            return Err(format!("Home Assistant request failed: {}", message["error"]).into());
        }
        return Ok(message["result"].clone());
    }
}

fn load_config() -> Result<(String, String), Error> {
    let env_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".env");
    let contents = match std::fs::read_to_string(&env_path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };
    let file_values = contents
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            let value = value.trim().trim_matches('"').trim_matches('\'').to_owned();
            Some((key.trim().to_owned(), value))
        })
        .collect::<HashMap<_, _>>();

    let value = |key: &str| {
        std::env::var(key)
            .ok()
            .or_else(|| file_values.get(key).cloned())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                format!("{key} is missing from the environment and {}", env_path.display())
            })
    };

    let base_url = value("HA_BASE_URL")?;
    let token = value("HA_TOKEN")?;
    let websocket_base = if let Some(host) = base_url.strip_prefix("https://") {
        format!("wss://{host}")
    } else if let Some(host) = base_url.strip_prefix("http://") {
        format!("ws://{host}")
    } else {
        return Err("HA_BASE_URL must use http:// or https://".into());
    };

    Ok((format!("{}/api/websocket", websocket_base.trim_end_matches('/')), token))
}

fn device_entities(devices: &Value, registry: &Value, states: &Value) -> Vec<HaEntity> {
    let device_names = devices
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|device| {
            let id = device["id"].as_str()?;
            let name = device["name_by_user"]
                .as_str()
                .or_else(|| device["name"].as_str())
                .or_else(|| device["model"].as_str())
                .unwrap_or(id);
            Some((id.to_owned(), name.to_owned()))
        })
        .collect::<HashMap<_, _>>();
    let states_by_id = states
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entity| Some((entity["entity_id"].as_str()?.to_owned(), entity)))
        .collect::<HashMap<_, _>>();

    registry
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entity| {
            if !entity["disabled_by"].is_null() {
                return None;
            }
            let entity_id = entity["entity_id"].as_str()?;
            let device_id = entity["device_id"].as_str()?;
            let current = states_by_id.get(entity_id)?;
            let domain = entity_id.split_once('.')?.0;
            Some(HaEntity {
                entity_id: entity_id.to_owned(),
                device_name: device_names
                    .get(device_id)
                    .cloned()
                    .unwrap_or_else(|| device_id.to_owned()),
                name: entity["name"]
                    .as_str()
                    .or_else(|| current["attributes"]["friendly_name"].as_str())
                    .unwrap_or(entity_id)
                    .to_owned(),
                state: current["state"].as_str().unwrap_or("unknown").to_owned(),
                toggleable: matches!(
                    domain,
                    "light" | "switch" | "input_boolean" | "fan" | "media_player"
                ),
            })
        })
        .collect()
}

fn state_from_event(message: &Value) -> Option<(String, String)> {
    let data = &message["event"]["data"];
    Some((data["entity_id"].as_str()?.to_owned(), data["new_state"]["state"].as_str()?.to_owned()))
}

async fn request_initial_state(ws: &mut HaWebSocket) -> Result<Vec<HaEntity>, Error> {
    send_json(ws, json!({"id": 1, "type": "config/device_registry/list"})).await?;
    let devices = wait_result(ws, 1).await?;

    send_json(ws, json!({"id": 2, "type": "config/entity_registry/list"})).await?;
    let registry = wait_result(ws, 2).await?;

    send_json(ws, json!({"id": 3, "type": "get_states"})).await?;
    let states = wait_result(ws, 3).await?;

    Ok(device_entities(&devices, &registry, &states))
}

fn service_for(change: &StateChange) -> Result<(&str, &str), Error> {
    let (domain, _) = change
        .entity_id
        .split_once('.')
        .ok_or_else(|| format!("Invalid entity ID: {}", change.entity_id))?;
    if change.state != "on" && change.state != "off" {
        return Err(format!("Unsupported requested state: {}", change.state).into());
    }
    if !matches!(domain, "light" | "switch" | "input_boolean" | "fan" | "media_player") {
        return Err(format!("State changes are not supported for {domain} entities").into());
    }
    Ok((domain, if change.state == "on" { "turn_on" } else { "turn_off" }))
}

pub async fn run(state: SharedState) -> Result<(), Error> {
    state.set_status("Loading Home Assistant configuration...");
    let (url, token) = load_config()?;
    state.set_status("Connecting to Home Assistant...");
    let (mut ws, _) = connect_async(url).await?;

    let challenge = recv_json(&mut ws).await?;
    if challenge["type"] != "auth_required" {
        return Err("Home Assistant did not request WebSocket authentication".into());
    }

    send_json(&mut ws, json!({"type": "auth", "access_token": token})).await?;
    let auth = recv_json(&mut ws).await?;
    if auth["type"] != "auth_ok" {
        return Err("Home Assistant rejected the WebSocket credentials".into());
    }

    state.set_status("Loading devices...");
    let entities = request_initial_state(&mut ws).await?;
    state.set_entities(entities);

    send_json(&mut ws, json!({"id": 4, "type": "subscribe_events", "event_type": "state_changed"}))
        .await?;
    wait_result(&mut ws, 4).await?;
    state.set_status("Connected to Home Assistant");

    let mut next_id = 5u64;
    let mut pending_results = HashSet::new();
    let mut command_timer = tokio::time::interval(std::time::Duration::from_millis(100));

    loop {
        tokio::select! {
            message = ws.next() => {
                let message = message.ok_or("Home Assistant closed the WebSocket")??;
                let Message::Text(text) = message else {
                    if matches!(message, Message::Close(_)) {
                        return Err("Home Assistant closed the WebSocket".into());
                    }
                    continue;
                };
                let message: Value = serde_json::from_str(text.as_str())?;

                if message["type"] == "event" && message["id"] == 4 {
                    if let Some((entity_id, value)) = state_from_event(&message) {
                        state.update_entity_state(&entity_id, &value);
                    }
                } else if message["type"] == "result" {
                    match message["id"].as_u64() {
                        Some(id) if pending_results.remove(&id) => {
                            if message["success"] != true {
                                let error = format!("State change failed: {}", message["error"]);
                                eprintln!("Home Assistant {error}");
                                state.set_status(format!("Connected; {error}"));
                            } else {
                                state.set_status("Connected to Home Assistant");
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ = command_timer.tick() => {
                for change in state.take_pending_changes() {
                    let (domain, service) = match service_for(&change) {
                        Ok(service) => service,
                        Err(error) => {
                            eprintln!("Could not send Home Assistant state change: {error}");
                            state.set_status(format!("Connected; state change failed: {error}"));
                            continue;
                        }
                    };
                    let id = next_id;
                    next_id += 1;
                    send_json(&mut ws, json!({
                        "id": id,
                        "type": "call_service",
                        "domain": domain,
                        "service": service,
                        "service_data": {"entity_id": change.entity_id},
                    })).await?;
                    pending_results.insert(id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{device_entities, service_for};
    use crate::state::StateChange;
    use serde_json::json;

    #[test]
    fn device_entities_include_registered_entity_state() {
        let devices = json!([{"id": "device-1", "name": "Kitchen"}]);
        let registry = json!([{
            "entity_id": "light.ceiling",
            "device_id": "device-1",
            "name": null,
            "disabled_by": null
        }]);
        let states = json!([{
            "entity_id": "light.ceiling",
            "state": "on",
            "attributes": {"friendly_name": "Ceiling"}
        }]);

        let entities = device_entities(&devices, &registry, &states);

        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].device_name, "Kitchen");
        assert_eq!(entities[0].name, "Ceiling");
        assert_eq!(entities[0].state, "on");
        assert!(entities[0].toggleable);
    }

    #[test]
    fn state_change_requests_map_to_home_assistant_services() {
        let change = StateChange { entity_id: "light.ceiling".into(), state: "off".into() };

        assert_eq!(service_for(&change).unwrap(), ("light", "turn_off"));
    }

    #[test]
    fn state_change_requests_reject_unsupported_domains() {
        let change = StateChange { entity_id: "sensor.temperature".into(), state: "on".into() };

        assert!(service_for(&change).is_err());
    }
}
