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

fn next_request_id(counter: &mut u64) -> Result<u64, Error> {
    let id = *counter;
    *counter = counter.checked_add(1).ok_or("Home Assistant request ID counter overflowed")?;
    Ok(id)
}

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

fn device_entities(
    devices: &Value,
    registry: &Value,
    areas: &Value,
    states: &Value,
) -> Vec<HaEntity> {
    let devices_by_id = devices
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|device| {
            let id = device["id"].as_str()?;
            Some((id.to_owned(), device))
        })
        .collect::<HashMap<_, _>>();
    let area_names = areas
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|area| {
            Some((area["area_id"].as_str()?.to_owned(), area["name"].as_str()?.to_owned()))
        })
        .collect::<HashMap<_, _>>();
    let device_names = devices_by_id
        .iter()
        .map(|(id, device)| {
            let name = device["name_by_user"]
                .as_str()
                .or_else(|| device["name"].as_str())
                .or_else(|| device["model"].as_str())
                .unwrap_or(id);
            (id.clone(), name.to_owned())
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
            let attributes = &current["attributes"];
            let dimmable = if let Some(color_modes) = attributes["supported_color_modes"].as_array()
            {
                color_modes.iter().any(|mode| {
                    mode.as_str().is_some_and(|mode| mode != "onoff" && mode != "unknown")
                })
            } else {
                attributes["brightness"].as_u64().is_some()
            };
            let area_id = entity["area_id"].as_str().or_else(|| {
                devices_by_id.get(device_id).and_then(|device| device["area_id"].as_str())
            });
            Some(HaEntity {
                entity_id: entity_id.to_owned(),
                device_name: device_names
                    .get(device_id)
                    .cloned()
                    .unwrap_or_else(|| device_id.to_owned()),
                area_name: area_id
                    .and_then(|area_id| area_names.get(area_id))
                    .cloned()
                    .unwrap_or_else(|| "Unassigned".to_owned()),
                domain: domain.to_owned(),
                name: entity["name"]
                    .as_str()
                    .or_else(|| current["attributes"]["friendly_name"].as_str())
                    .unwrap_or(entity_id)
                    .to_owned(),
                state: current["state"].as_str().unwrap_or("unknown").to_owned(),
                brightness: attributes["brightness"]
                    .as_u64()
                    .and_then(|value| u8::try_from(value).ok())
                    .unwrap_or_else(|| if current["state"] == "on" { u8::MAX } else { 0 }),
                dimmable,
                toggleable: matches!(
                    domain,
                    "light" | "switch" | "input_boolean" | "fan" | "media_player"
                ),
            })
        })
        .collect()
}

fn state_from_event(message: &Value) -> Option<(String, String, Option<u8>)> {
    let data = &message["event"]["data"];
    let new_state = &data["new_state"];
    Some((
        data["entity_id"].as_str()?.to_owned(),
        new_state["state"].as_str()?.to_owned(),
        new_state["attributes"]["brightness"].as_u64().and_then(|value| u8::try_from(value).ok()),
    ))
}

async fn request_initial_state(
    ws: &mut HaWebSocket,
    request_counter: &mut u64,
) -> Result<Vec<HaEntity>, Error> {
    let id = next_request_id(request_counter)?;
    send_json(ws, json!({"id": id, "type": "config/device_registry/list"})).await?;
    let devices = wait_result(ws, id).await?;

    let id = next_request_id(request_counter)?;
    send_json(ws, json!({"id": id, "type": "config/entity_registry/list"})).await?;
    let registry = wait_result(ws, id).await?;

    let id = next_request_id(request_counter)?;
    send_json(ws, json!({"id": id, "type": "config/area_registry/list"})).await?;
    let areas = wait_result(ws, id).await?;

    let id = next_request_id(request_counter)?;
    send_json(ws, json!({"id": id, "type": "get_states"})).await?;
    let states = wait_result(ws, id).await?;

    Ok(device_entities(&devices, &registry, &areas, &states))
}

fn service_for(change: &StateChange) -> Result<(&str, &str), Error> {
    let (domain, _) = change
        .entity_id
        .split_once('.')
        .ok_or_else(|| format!("Invalid entity ID: {}", change.entity_id))?;
    if change.brightness.is_some() && domain != "light" {
        return Err(format!("Brightness changes are not supported for {domain} entities").into());
    }
    if change.brightness.is_none() && change.state != "on" && change.state != "off" {
        return Err(format!("Unsupported requested state: {}", change.state).into());
    }
    if !matches!(domain, "light" | "switch" | "input_boolean" | "fan" | "media_player") {
        return Err(format!("State changes are not supported for {domain} entities").into());
    }
    Ok((
        domain,
        if change.brightness.is_some() || change.state == "on" { "turn_on" } else { "turn_off" },
    ))
}

pub async fn run(state: SharedState) -> Result<(), Error> {
    let mut request_counter = 1u64;
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
    let entities = request_initial_state(&mut ws, &mut request_counter).await?;
    state.set_entities(entities);

    let subscription_id = next_request_id(&mut request_counter)?;
    send_json(
        &mut ws,
        json!({"id": subscription_id, "type": "subscribe_events", "event_type": "state_changed"}),
    )
    .await?;
    wait_result(&mut ws, subscription_id).await?;
    state.set_status("Connected to Home Assistant");

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

                if message["type"] == "event" && message["id"] == subscription_id {
                    if let Some((entity_id, value, brightness)) = state_from_event(&message) {
                        state.update_entity_state(&entity_id, &value, brightness);
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
                    let id = next_request_id(&mut request_counter)?;
                    let mut service_data = json!({"entity_id": change.entity_id});
                    if let Some(brightness) = change.brightness {
                        service_data["brightness"] = json!(brightness);
                    }
                    send_json(&mut ws, json!({
                        "id": id,
                        "type": "call_service",
                        "domain": domain,
                        "service": service,
                        "service_data": service_data,
                    })).await?;
                    pending_results.insert(id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{device_entities, next_request_id, service_for, state_from_event};
    use crate::state::StateChange;
    use serde_json::json;

    #[test]
    fn request_ids_increment_for_every_request() {
        let mut counter = 1;

        assert_eq!(next_request_id(&mut counter).unwrap(), 1);
        assert_eq!(next_request_id(&mut counter).unwrap(), 2);
        assert_eq!(next_request_id(&mut counter).unwrap(), 3);
    }

    #[test]
    fn request_ids_restart_for_a_new_connection() {
        let mut reconnected_counter = 1;

        assert_eq!(next_request_id(&mut reconnected_counter).unwrap(), 1);
    }

    #[test]
    fn device_entities_include_registered_entity_state() {
        let devices = json!([{
            "id": "device-1",
            "name": "Kitchen light",
            "area_id": "area-device"
        }]);
        let registry = json!([{
            "entity_id": "light.ceiling",
            "device_id": "device-1",
            "area_id": "area-entity",
            "name": null,
            "disabled_by": null
        }]);
        let areas = json!([
            {"area_id": "area-device", "name": "Kitchen"},
            {"area_id": "area-entity", "name": "Dining Room"}
        ]);
        let states = json!([{
            "entity_id": "light.ceiling",
            "state": "on",
            "attributes": {
                "friendly_name": "Ceiling",
                "brightness": 128,
                "supported_color_modes": ["brightness"]
            }
        }]);

        let entities = device_entities(&devices, &registry, &areas, &states);

        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].device_name, "Kitchen light");
        assert_eq!(entities[0].area_name, "Dining Room");
        assert_eq!(entities[0].name, "Ceiling");
        assert_eq!(entities[0].state, "on");
        assert_eq!(entities[0].brightness, 128);
        assert!(entities[0].dimmable);
        assert!(entities[0].toggleable);
    }

    #[test]
    fn onoff_lights_are_not_marked_dimmable() {
        let devices = json!([{"id": "device-1", "name": "Porch light"}]);
        let registry = json!([{
            "entity_id": "light.porch",
            "device_id": "device-1",
            "disabled_by": null
        }]);
        let states = json!([{
            "entity_id": "light.porch",
            "state": "on",
            "attributes": {
                "brightness": 255,
                "supported_color_modes": ["onoff"]
            }
        }]);

        let entities = device_entities(&devices, &registry, &json!([]), &states);

        assert!(!entities[0].dimmable);
    }

    #[test]
    fn state_changed_events_include_light_brightness() {
        let event = json!({
            "event": {
                "data": {
                    "entity_id": "light.ceiling",
                    "new_state": {
                        "state": "on",
                        "attributes": {"brightness": 128}
                    }
                }
            }
        });

        assert_eq!(
            state_from_event(&event),
            Some(("light.ceiling".into(), "on".into(), Some(128)))
        );
    }

    #[test]
    fn device_area_is_used_when_entity_has_no_area() {
        let devices = json!([{
            "id": "device-1",
            "name": "Kitchen light",
            "area_id": "area-kitchen"
        }]);
        let registry = json!([{
            "entity_id": "light.ceiling",
            "device_id": "device-1",
            "area_id": null,
            "disabled_by": null
        }]);
        let areas = json!([{"area_id": "area-kitchen", "name": "Kitchen"}]);
        let states = json!([{"entity_id": "light.ceiling", "state": "off"}]);

        let entities = device_entities(&devices, &registry, &areas, &states);

        assert_eq!(entities[0].area_name, "Kitchen");
    }

    #[test]
    fn devices_without_an_area_are_marked_unassigned() {
        let devices = json!([{"id": "device-1", "name": "Lamp"}]);
        let registry = json!([{
            "entity_id": "light.lamp",
            "device_id": "device-1",
            "area_id": null,
            "disabled_by": null
        }]);
        let states = json!([{"entity_id": "light.lamp", "state": "off"}]);

        let entities = device_entities(&devices, &registry, &json!([]), &states);

        assert_eq!(entities[0].area_name, "Unassigned");
    }

    #[test]
    fn state_change_requests_map_to_home_assistant_services() {
        let change = StateChange {
            entity_id: "light.ceiling".into(),
            state: "off".into(),
            brightness: None,
        };

        assert_eq!(service_for(&change).unwrap(), ("light", "turn_off"));
    }

    #[test]
    fn brightness_requests_use_the_light_turn_on_service() {
        let change = StateChange {
            entity_id: "light.ceiling".into(),
            state: "on".into(),
            brightness: Some(128),
        };

        assert_eq!(service_for(&change).unwrap(), ("light", "turn_on"));
    }

    #[test]
    fn state_change_requests_reject_unsupported_domains() {
        let change = StateChange {
            entity_id: "sensor.temperature".into(),
            state: "on".into(),
            brightness: None,
        };

        assert!(service_for(&change).is_err());
    }
}
