// Copyright © SixtyFPS GmbH <info@slint.dev>
// SPDX-License-Identifier: MIT

use crate::state::{HaEntity, HomeAssistantAction, SharedState, StateChange, WeatherForecast};
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

fn load_config() -> Result<(String, String, Option<String>), Error> {
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

    let weather_entity = std::env::var("HA_WEATHER_ENTITY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| file_values.get("HA_WEATHER_ENTITY").cloned())
        .filter(|value| !value.trim().is_empty());

    Ok((format!("{}/api/websocket", websocket_base.trim_end_matches('/')), token, weather_entity))
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
            let current = states_by_id.get(entity_id)?;
            let domain = entity_id.split_once('.')?.0;
            let device_id = entity["device_id"].as_str();
            let attributes = &current["attributes"];
            let supported_color_modes = attributes["supported_color_modes"].as_array();
            let dimmable = if let Some(color_modes) = supported_color_modes {
                color_modes.iter().any(|mode| {
                    mode.as_str().is_some_and(|mode| mode != "onoff" && mode != "unknown")
                })
            } else {
                attributes["brightness"].as_u64().is_some()
            };
            let color_capable = supported_color_modes.is_some_and(|color_modes| {
                color_modes.iter().any(|mode| {
                    matches!(mode.as_str(), Some("hs" | "xy" | "rgb" | "rgbw" | "rgbww"))
                })
            }) || attributes["hs_color"].is_array()
                || attributes["rgb_color"].is_array();
            let (hue, saturation) = color_from_attributes(attributes).unwrap_or((0, 100));
            let area_id = entity["area_id"].as_str().or_else(|| {
                device_id
                    .and_then(|device_id| devices_by_id.get(device_id))
                    .and_then(|device| device["area_id"].as_str())
            });
            Some(HaEntity {
                entity_id: entity_id.to_owned(),
                device_name: device_id
                    .and_then(|device_id| device_names.get(device_id))
                    .cloned()
                    .unwrap_or_else(|| {
                        entity["name"]
                            .as_str()
                            .or_else(|| attributes["friendly_name"].as_str())
                            .unwrap_or(entity_id)
                            .to_owned()
                    }),
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
                color_capable,
                hue,
                saturation,
                toggleable: matches!(
                    domain,
                    "light" | "switch" | "input_boolean" | "fan" | "media_player"
                ),
            })
        })
        .collect()
}

fn color_from_attributes(attributes: &Value) -> Option<(u16, u8)> {
    if let Some(hs_color) = attributes["hs_color"].as_array() {
        let hue = hs_color.first()?.as_f64()?;
        let saturation = hs_color.get(1)?.as_f64()?;
        if !hue.is_finite() || !saturation.is_finite() {
            return None;
        }
        return Some((
            hue.rem_euclid(360.0).round() as u16 % 360,
            saturation.clamp(0.0, 100.0).round() as u8,
        ));
    }

    let rgb = attributes["rgb_color"].as_array()?;
    let red = (rgb.first()?.as_f64()? / 255.0).clamp(0.0, 1.0);
    let green = (rgb.get(1)?.as_f64()? / 255.0).clamp(0.0, 1.0);
    let blue = (rgb.get(2)?.as_f64()? / 255.0).clamp(0.0, 1.0);
    let maximum = red.max(green).max(blue);
    let minimum = red.min(green).min(blue);
    let delta = maximum - minimum;
    let hue = if delta == 0.0 {
        0.0
    } else if maximum == red {
        60.0 * ((green - blue) / delta).rem_euclid(6.0)
    } else if maximum == green {
        60.0 * ((blue - red) / delta + 2.0)
    } else {
        60.0 * ((red - green) / delta + 4.0)
    };
    let saturation = if maximum == 0.0 { 0.0 } else { delta / maximum * 100.0 };
    Some((hue.round() as u16 % 360, saturation.round().clamp(0.0, 100.0) as u8))
}

fn state_from_event(message: &Value) -> Option<(String, String, Option<u8>, Option<(u16, u8)>)> {
    let data = &message["event"]["data"];
    let new_state = &data["new_state"];
    Some((
        data["entity_id"].as_str()?.to_owned(),
        new_state["state"].as_str()?.to_owned(),
        new_state["attributes"]["brightness"].as_u64().and_then(|value| u8::try_from(value).ok()),
        color_from_attributes(&new_state["attributes"]),
    ))
}

async fn request_initial_state(
    ws: &mut HaWebSocket,
    request_counter: &mut u64,
) -> Result<(Vec<HaEntity>, Value), Error> {
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

    Ok((device_entities(&devices, &registry, &areas, &states), states))
}

fn select_weather_entity(
    entities: &[HaEntity],
    configured: Option<&str>,
) -> Result<Option<String>, Error> {
    let weather_entities = entities
        .iter()
        .filter(|entity| entity.domain == "weather")
        .map(|entity| entity.entity_id.clone())
        .collect::<Vec<_>>();

    if let Some(entity_id) = configured {
        if !entity_id.starts_with("weather.") || !weather_entities.iter().any(|id| id == entity_id)
        {
            return Err(format!(
                "HA_WEATHER_ENTITY '{entity_id}' is not an available Home Assistant weather entity"
            )
            .into());
        }
        return Ok(Some(entity_id.to_owned()));
    }

    let mut weather_entities = weather_entities;
    weather_entities.sort();
    Ok(weather_entities.into_iter().next())
}

fn weather_temperature_unit(states: &Value, entity_id: &str) -> String {
    states
        .as_array()
        .into_iter()
        .flatten()
        .find(|entity| entity["entity_id"] == entity_id)
        .and_then(|entity| entity["attributes"]["temperature_unit"].as_str())
        .unwrap_or("°C")
        .to_owned()
}

fn forecast_day(datetime: &str, index: usize) -> String {
    chrono::DateTime::parse_from_rfc3339(datetime)
        .map(|date| date.format("%a").to_string())
        .or_else(|_| {
            chrono::NaiveDate::parse_from_str(datetime.get(..10).unwrap_or_default(), "%Y-%m-%d")
                .map(|date| date.format("%a").to_string())
        })
        .unwrap_or_else(|_| if index == 0 { "Today".to_owned() } else { "—".to_owned() })
}

fn parse_weather_forecast(result: &Value, entity_id: &str) -> Result<Vec<WeatherForecast>, Error> {
    let forecast = result["response"][entity_id]["forecast"]
        .as_array()
        .ok_or_else(|| format!("Home Assistant returned no forecast for {entity_id}"))?;

    let mut forecast = forecast
        .iter()
        .take(5)
        .enumerate()
        .map(|(index, item)| {
            let temperature = item["temperature"]
                .as_f64()
                .map(|temperature| format!("{:.0}", temperature.round()))
                .unwrap_or_else(|| "—".to_owned());
            WeatherForecast {
                condition: item["condition"].as_str().unwrap_or("cloudy").to_owned(),
                day: item["datetime"]
                    .as_str()
                    .map(|datetime| forecast_day(datetime, index))
                    .unwrap_or_else(
                        || if index == 0 { "Today".to_owned() } else { "—".to_owned() },
                    ),
                temperature,
            }
        })
        .collect::<Vec<_>>();

    while forecast.len() < 5 {
        forecast.push(WeatherForecast::default());
    }
    Ok(forecast)
}

async fn request_weather_forecast(
    ws: &mut HaWebSocket,
    request_counter: &mut u64,
    entity_id: &str,
    temperature_unit: &str,
) -> Result<(Vec<WeatherForecast>, String), Error> {
    let id = next_request_id(request_counter)?;
    send_json(
        ws,
        json!({
            "id": id,
            "type": "call_service",
            "domain": "weather",
            "service": "get_forecasts",
            "service_data": {"type": "daily"},
            "target": {"entity_id": entity_id},
            "return_response": true,
        }),
    )
    .await?;
    let result = wait_result(ws, id).await?;
    let forecast = parse_weather_forecast(&result, entity_id)?;
    Ok((forecast, temperature_unit.to_owned()))
}

async fn refresh_weather_forecast(
    ws: &mut HaWebSocket,
    request_counter: &mut u64,
    state: &SharedState,
    entity_id: &str,
    temperature_unit: &str,
) {
    match request_weather_forecast(ws, request_counter, entity_id, temperature_unit).await {
        Ok((forecast, temperature_unit)) => {
            state.set_weather_forecast(forecast, temperature_unit);
        }
        Err(error) => {
            let message = format!("Could not load weather forecast: {error}");
            eprintln!("{message}");
            state.set_weather_status(message);
        }
    }
}

fn service_for(action: &HomeAssistantAction) -> Result<(&str, &str), Error> {
    let (entity_id, expected_domain) = match action {
        HomeAssistantAction::StateChange(change) => (&change.entity_id, None),
        HomeAssistantAction::ColorChange { entity_id, .. } => (entity_id, Some("light")),
        HomeAssistantAction::ActivateScene(entity_id) => (entity_id, Some("scene")),
        HomeAssistantAction::TimerAction { entity_id, .. } => (entity_id, Some("timer")),
    };
    let (domain, object_id) = entity_id
        .split_once('.')
        .filter(|(domain, object_id)| !domain.is_empty() && !object_id.is_empty())
        .ok_or_else(|| format!("Invalid entity ID: {entity_id}"))?;

    match action {
        HomeAssistantAction::StateChange(change) => {
            if change.brightness.is_some() && domain != "light" {
                return Err(
                    format!("Brightness changes are not supported for {domain} entities").into()
                );
            }
            if change.brightness.is_none() && change.state != "on" && change.state != "off" {
                return Err(format!("Unsupported requested state: {}", change.state).into());
            }
            if !matches!(domain, "light" | "switch" | "input_boolean" | "fan" | "media_player") {
                return Err(format!("State changes are not supported for {domain} entities").into());
            }
            Ok((
                domain,
                if change.brightness.is_some() || change.state == "on" {
                    "turn_on"
                } else {
                    "turn_off"
                },
            ))
        }
        HomeAssistantAction::ActivateScene(_) if expected_domain == Some(domain) => {
            Ok((domain, "turn_on"))
        }
        HomeAssistantAction::ColorChange { .. } if expected_domain == Some(domain) => {
            Ok((domain, "turn_on"))
        }
        HomeAssistantAction::TimerAction { action, .. } if expected_domain == Some(domain) => {
            match action.as_str() {
                "start" | "pause" | "cancel" => Ok((domain, action)),
                _ => Err(format!("Unsupported timer action: {action}").into()),
            }
        }
        HomeAssistantAction::ActivateScene(_) => {
            Err(format!("Scene activation is not supported for {domain}.{object_id}").into())
        }
        HomeAssistantAction::TimerAction { .. } => {
            Err(format!("Timer actions are not supported for {domain}.{object_id}").into())
        }
        HomeAssistantAction::ColorChange { .. } => {
            Err(format!("Color changes are not supported for {domain}.{object_id}").into())
        }
    }
}

fn service_data_for(entity_id: &str, action: &HomeAssistantAction) -> Value {
    let mut service_data = json!({"entity_id": entity_id});
    match action {
        HomeAssistantAction::StateChange(StateChange { brightness: Some(brightness), .. }) => {
            service_data["brightness"] = json!(brightness);
        }
        HomeAssistantAction::ColorChange { hue, saturation, .. } => {
            service_data["hs_color"] = json!([hue, saturation]);
        }
        _ => {}
    }
    service_data
}

pub async fn run(state: SharedState) -> Result<(), Error> {
    let mut request_counter = 1u64;
    state.set_status("Loading Home Assistant configuration...");
    let (url, token, configured_weather_entity) = load_config()?;
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
    let (entities, states) = request_initial_state(&mut ws, &mut request_counter).await?;
    let weather_entity =
        match select_weather_entity(&entities, configured_weather_entity.as_deref()) {
            Ok(weather_entity) => weather_entity,
            Err(error) => {
                let message = error.to_string();
                eprintln!("{message}");
                state.set_weather_status(message);
                None
            }
        };
    state.set_entities(entities);
    let weather_unit = weather_entity
        .as_deref()
        .map(|entity_id| weather_temperature_unit(&states, entity_id))
        .unwrap_or_else(|| "°C".to_owned());
    if let Some(entity_id) = &weather_entity {
        refresh_weather_forecast(&mut ws, &mut request_counter, &state, entity_id, &weather_unit)
            .await;
    } else if state.snapshot().weather_status == "Waiting for Home Assistant weather..." {
        state.set_weather_status("No Home Assistant weather entity found");
    }

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
    let mut weather_timer = tokio::time::interval_at(
        tokio::time::Instant::now() + std::time::Duration::from_secs(30 * 60),
        std::time::Duration::from_secs(30 * 60),
    );

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
                    if let Some((entity_id, value, brightness, color)) = state_from_event(&message) {
                        state.update_entity_state(&entity_id, &value, brightness, color);
                        if weather_entity.as_deref() == Some(entity_id.as_str()) {
                            refresh_weather_forecast(
                                &mut ws,
                                &mut request_counter,
                                &state,
                                &entity_id,
                                &weather_unit,
                            )
                            .await;
                        }
                    }
                } else if message["type"] == "result" {
                    match message["id"].as_u64() {
                        Some(id) if pending_results.remove(&id) => {
                            if message["success"] != true {
                                let error = format!("Home Assistant action failed: {}", message["error"]);
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
            _ = weather_timer.tick(), if weather_entity.is_some() => {
                if let Some(entity_id) = &weather_entity {
                    refresh_weather_forecast(
                        &mut ws,
                        &mut request_counter,
                        &state,
                        entity_id,
                        &weather_unit,
                    )
                    .await;
                }
            }
            _ = command_timer.tick() => {
                for change in state.take_pending_changes() {
                    let (domain, service) = match service_for(&change) {
                        Ok(service) => service,
                        Err(error) => {
                            eprintln!("Could not send Home Assistant action: {error}");
                            state.set_status(format!("Connected; action failed: {error}"));
                            continue;
                        }
                    };
                    let id = next_request_id(&mut request_counter)?;
                    let entity_id = match &change {
                        HomeAssistantAction::StateChange(change) => &change.entity_id,
                        HomeAssistantAction::ColorChange { entity_id, .. } => entity_id,
                        HomeAssistantAction::ActivateScene(entity_id)
                        | HomeAssistantAction::TimerAction { entity_id, .. } => entity_id,
                    };
                    let service_data = service_data_for(entity_id, &change);
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
    use super::{
        color_from_attributes, device_entities, next_request_id, parse_weather_forecast,
        select_weather_entity, service_data_for,
        service_for, state_from_event, weather_temperature_unit,
    };
    use crate::state::{HaEntity, HomeAssistantAction, StateChange};
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
                "supported_color_modes": ["brightness", "hs"],
                "hs_color": [210.0, 55.0]
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
        assert!(entities[0].color_capable);
        assert_eq!((entities[0].hue, entities[0].saturation), (210, 55));
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
        assert!(!entities[0].color_capable);
    }

    #[test]
    fn supported_color_modes_mark_lights_color_capable() {
        let registry = json!([{"entity_id": "light.rgb", "disabled_by": null}]);
        let states = json!([{
            "entity_id": "light.rgb",
            "state": "off",
            "attributes": {"supported_color_modes": ["rgb"]}
        }]);

        let entities = device_entities(&json!([]), &registry, &json!([]), &states);

        assert!(entities[0].color_capable);
    }

    #[test]
    fn state_changed_events_include_light_brightness() {
        let event = json!({
            "event": {
                "data": {
                    "entity_id": "light.ceiling",
                    "new_state": {
                        "state": "on",
                        "attributes": {"brightness": 128, "hs_color": [210.0, 55.0]}
                    }
                }
            }
        });

        assert_eq!(
            state_from_event(&event),
            Some(("light.ceiling".into(), "on".into(), Some(128), Some((210, 55))))
        );
    }

    #[test]
    fn rgb_color_attributes_are_converted_to_hs() {
        assert_eq!(color_from_attributes(&json!({"rgb_color": [0, 128, 255]})), Some((210, 100)));
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
    fn entities_without_a_device_are_included() {
        let registry = json!([{
            "entity_id": "scene.evening",
            "name": "Evening",
            "disabled_by": null
        }]);
        let states = json!([{"entity_id": "scene.evening", "state": "scening"}]);

        let entities = device_entities(&json!([]), &registry, &json!([]), &states);

        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].area_name, "Unassigned");
        assert_eq!(entities[0].name, "Evening");
        assert_eq!(entities[0].domain, "scene");
    }

    #[test]
    fn state_change_requests_map_to_home_assistant_services() {
        let change = HomeAssistantAction::StateChange(StateChange {
            entity_id: "light.ceiling".into(),
            state: "off".into(),
            brightness: None,
        });

        assert_eq!(service_for(&change).unwrap(), ("light", "turn_off"));
    }

    #[test]
    fn brightness_requests_use_the_light_turn_on_service() {
        let change = HomeAssistantAction::StateChange(StateChange {
            entity_id: "light.ceiling".into(),
            state: "on".into(),
            brightness: Some(128),
        });

        assert_eq!(service_for(&change).unwrap(), ("light", "turn_on"));
    }

    #[test]
    fn color_requests_use_the_light_turn_on_service() {
        let action = HomeAssistantAction::ColorChange {
            entity_id: "light.ceiling".into(),
            hue: 210,
            saturation: 55,
        };

        assert_eq!(service_for(&action).unwrap(), ("light", "turn_on"));
    }

    #[test]
    fn color_requests_send_hs_color_in_service_data() {
        let action = HomeAssistantAction::ColorChange {
            entity_id: "light.ceiling".into(),
            hue: 210,
            saturation: 55,
        };

        assert_eq!(
            service_data_for("light.ceiling", &action),
            json!({"entity_id": "light.ceiling", "hs_color": [210, 55]})
        );
    }

    #[test]
    fn color_requests_reject_non_light_entities() {
        let action = HomeAssistantAction::ColorChange {
            entity_id: "switch.porch".into(),
            hue: 210,
            saturation: 55,
        };

        assert!(service_for(&action).is_err());
    }

    #[test]
    fn state_change_requests_reject_unsupported_domains() {
        let change = HomeAssistantAction::StateChange(StateChange {
            entity_id: "sensor.temperature".into(),
            state: "on".into(),
            brightness: None,
        });

        assert!(service_for(&change).is_err());
    }

    #[test]
    fn scene_activation_uses_the_scene_turn_on_service() {
        let action = HomeAssistantAction::ActivateScene("scene.evening".into());

        assert_eq!(service_for(&action).unwrap(), ("scene", "turn_on"));
    }

    #[test]
    fn timer_actions_use_only_supported_timer_services() {
        for action in ["start", "pause", "cancel"] {
            let request = HomeAssistantAction::TimerAction {
                entity_id: "timer.cooking".into(),
                action: action.into(),
            };
            assert_eq!(service_for(&request).unwrap(), ("timer", action));
        }

        let invalid = HomeAssistantAction::TimerAction {
            entity_id: "timer.cooking".into(),
            action: "finish".into(),
        };
        assert!(service_for(&invalid).is_err());
    }

    #[test]
    fn weather_forecasts_are_parsed_and_padded_to_five_days() {
        let response = json!({
            "response": {
                "weather.home": {
                    "forecast": [
                        {
                            "datetime": "2026-11-12T12:00:00+07:00",
                            "condition": "partlycloudy",
                            "temperature": 24.5
                        }
                    ]
                }
            }
        });

        let forecast = parse_weather_forecast(&response, "weather.home").unwrap();

        assert_eq!(forecast.len(), 5);
        assert_eq!(forecast[0].day, "Thu");
        assert_eq!(forecast[0].condition, "partlycloudy");
        assert_eq!(forecast[0].temperature, "25");
        assert!(forecast[1].day.is_empty());
    }

    #[test]
    fn weather_forecast_rejects_missing_entity_response() {
        let response = json!({"response": {}});

        assert!(parse_weather_forecast(&response, "weather.home").is_err());
    }

    #[test]
    fn weather_entity_can_be_configured_or_discovered() {
        let entities = vec![
            HaEntity {
                entity_id: "weather.z_home".into(),
                domain: "weather".into(),
                ..HaEntity::default()
            },
            HaEntity {
                entity_id: "weather.a_home".into(),
                domain: "weather".into(),
                ..HaEntity::default()
            },
        ];
        let states = json!([
            {"entity_id": "weather.z_home", "attributes": {"temperature_unit": "°F"}},
            {"entity_id": "weather.a_home", "attributes": {"temperature_unit": "°C"}},
        ]);

        assert_eq!(
            select_weather_entity(&entities, None).unwrap().as_deref(),
            Some("weather.a_home")
        );
        assert_eq!(
            select_weather_entity(&entities, Some("weather.z_home")).unwrap().as_deref(),
            Some("weather.z_home")
        );
        assert!(select_weather_entity(&entities, Some("sensor.outdoor")).is_err());
        assert_eq!(weather_temperature_unit(&states, "weather.z_home"), "°F");
    }
}
