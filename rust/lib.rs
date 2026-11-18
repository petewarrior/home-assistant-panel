// Copyright © SixtyFPS GmbH <info@slint.dev>
// SPDX-License-Identifier: MIT

// cSpell: ignore Timelike
use chrono::{Datelike, Local, Timelike};
use slint::{Timer, TimerMode};

mod state;
#[cfg(not(target_arch = "wasm32"))]
mod ws;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(feature = "sw-renderer")]
slint::slint! {
    #[edition = "1.19"]
    export { Api, AppWindow } from "../ui/demo-sw-renderer.slint";
}

#[cfg(not(feature = "sw-renderer"))]
slint::slint! {
    #[edition = "1.19"]
    export { Api, AppWindow } from "../ui/demo.slint";
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(start))]
pub fn main() -> Result<(), slint::PlatformError> {
    // This provides better error messages in debug mode.
    // It's disabled in release mode so it doesn't bloat up the file size.
    #[cfg(all(debug_assertions, target_arch = "wasm32"))]
    console_error_panic_hook::set_once();

    let app = AppWindow::new()?;
    let app_weak = app.as_weak();
    let shared_state = state::SharedState::default();
    shared_state.set_status("Connecting to Home Assistant...");
    shared_state.set_weather_status("Waiting for Home Assistant weather...");
    #[cfg(target_arch = "wasm32")]
    {
        shared_state.set_status("Home Assistant is unavailable in browser builds");
        shared_state.set_weather_status("Weather is unavailable in browser builds");
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let websocket_state = shared_state.clone();
        if let Err(error) =
            std::thread::Builder::new().name("home-assistant-websocket".into()).spawn(move || {
                let runtime =
                    match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            websocket_state.set_status(format!(
                                "Could not start Home Assistant service: {error}"
                            ));
                            eprintln!("Could not start Home Assistant service: {error}");
                            return;
                        }
                    };

                loop {
                    if let Err(error) = runtime.block_on(ws::run(websocket_state.clone())) {
                        websocket_state
                            .set_status(format!("Home Assistant connection failed: {error}"));
                        websocket_state.set_weather_status(format!("Weather unavailable: {error}"));
                        eprintln!("Home Assistant connection failed: {error}");
                    }
                    std::thread::sleep(std::time::Duration::from_secs(5));
                }
            })
        {
            shared_state.set_status(format!("Could not start Home Assistant service: {error}"));
            eprintln!("Could not start Home Assistant service: {error}");
        }
    }

    let api = app.global::<Api>();
    let request_state = shared_state.clone();
    api.on_request_ha_state_change(move |entity_id, value| {
        request_state.request_state_change(entity_id.to_string(), value.to_string());
    });
    let request_brightness = shared_state.clone();
    api.on_request_ha_brightness_change(move |entity_id, brightness| {
        request_brightness.request_brightness_change(
            entity_id.to_string(),
            brightness.round().clamp(0.0, 255.0) as u8,
        );
    });
    let request_color = shared_state.clone();
    api.on_request_ha_color_change(move |entity_id, hue, saturation| {
        request_color.request_color_change(
            entity_id.to_string(),
            hue.clamp(0, 359) as u16,
            saturation.clamp(0, 100) as u8,
        );
    });
    let request_scene = shared_state.clone();
    api.on_request_ha_scene_activation(move |entity_id| {
        request_scene.request_scene_activation(entity_id.to_string());
    });
    let request_timer = shared_state.clone();
    api.on_request_ha_timer_action(move |entity_id, action| {
        request_timer.request_timer_action(entity_id.to_string(), action.to_string());
    });

    let mut displayed_snapshot = None;
    let timer = Timer::default();
    timer.start(TimerMode::Repeated, std::time::Duration::from_millis(500), move || {
        if let Some(app) = app_weak.upgrade() {
            let api = app.global::<Api>();
            let snapshot = shared_state.snapshot();
            if displayed_snapshot.as_ref() != Some(&snapshot) {
                let entities = snapshot.entities.iter().map(to_slint_entity).collect::<Vec<_>>();
                let rooms = snapshot
                    .rooms
                    .iter()
                    .map(|room| HaRoom {
                        name: room.name.as_str().into(),
                        entities: to_slint_entity_groups(&room.entities),
                    })
                    .collect::<Vec<_>>();
                api.set_ha_entities(slint::ModelRc::new(slint::VecModel::from(entities)));
                api.set_ha_rooms(slint::ModelRc::new(slint::VecModel::from(rooms)));
                api.set_ha_entity_groups(to_slint_entity_groups(&snapshot.entity_groups));
                api.set_ha_unassigned_entities(slint::ModelRc::new(slint::VecModel::from(
                    snapshot.unassigned_entities.iter().map(to_slint_entity).collect::<Vec<_>>(),
                )));
                api.set_ha_unassigned_entity_groups(to_slint_entity_groups(
                    &snapshot.unassigned_entity_groups,
                ));
                api.set_ha_status(snapshot.status.as_str().into());
                api.set_weather_forecast(slint::ModelRc::new(slint::VecModel::from(
                    snapshot
                        .weather_forecast
                        .iter()
                        .map(|forecast| WeatherData {
                            condition: to_slint_weather_condition(&forecast.condition),
                            day: forecast.day.as_str().into(),
                            temperature: forecast.temperature.as_str().into(),
                            available: !forecast.day.is_empty(),
                        })
                        .collect::<Vec<_>>(),
                )));
                api.set_weather_status(snapshot.weather_status.as_str().into());
                api.set_weather_temperature_unit(snapshot.weather_temperature_unit.as_str().into());
                displayed_snapshot = Some(snapshot);
            }

            fn to_slint_entity(entity: &state::HaEntity) -> HaEntity {
                HaEntity {
                    entity_id: entity.entity_id.as_str().into(),
                    device_name: entity.device_name.as_str().into(),
                    area_name: entity.area_name.as_str().into(),
                    domain: entity.domain.as_str().into(),
                    name: entity.name.as_str().into(),
                    state: entity.state.as_str().into(),
                    brightness: entity.brightness as i32,
                    dimmable: entity.dimmable,
                    color_capable: entity.color_capable,
                    hue: entity.hue as i32,
                    saturation: entity.saturation as i32,
                    toggleable: entity.toggleable,
                }
            }

            fn to_slint_entity_groups(groups: &state::HaEntityGroups) -> HaEntityGroups {
                let to_model = |entities: &[state::HaEntity]| {
                    slint::ModelRc::new(slint::VecModel::from(
                        entities.iter().map(to_slint_entity).collect::<Vec<_>>(),
                    ))
                };
                HaEntityGroups {
                    devices: to_model(&groups.devices),
                    scenes: to_model(&groups.scenes),
                    timers: to_model(&groups.timers),
                    other: to_model(&groups.other),
                }
            }

            fn to_slint_weather_condition(condition: &str) -> WeatherCondition {
                match condition {
                    "sunny" | "clear-night" => WeatherCondition::Sunny,
                    "partlycloudy" => WeatherCondition::SunnyCloudy,
                    "rainy" | "pouring" | "snowy" | "snowy-rainy" | "hail" => {
                        WeatherCondition::Rainy
                    }
                    "lightning-rainy" => WeatherCondition::SunnyRainy,
                    _ => WeatherCondition::Cloudy,
                }
            }

            let now = Local::now();
            let date = Date { year: now.year(), month: now.month() as i32, day: now.day() as i32 };
            api.set_current_date(date);

            let time = Time {
                hour: now.hour() as i32,
                minute: now.minute() as i32,
                second: now.second() as i32,
            };
            api.set_current_time(time);
        }
    });

    app.run()
}

#[cfg(target_os = "android")]
#[unsafe(no_mangle)]
fn android_main(android_app: slint::android::AndroidApp) {
    slint::android::init(android_app).unwrap();
    main().unwrap();
}
