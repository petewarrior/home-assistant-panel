// Copyright © SixtyFPS GmbH <info@slint.dev>
// SPDX-License-Identifier: MIT

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HaEntity {
    pub entity_id: String,
    pub device_name: String,
    pub area_name: String,
    pub domain: String,
    pub name: String,
    pub state: String,
    pub brightness: u8,
    pub dimmable: bool,
    pub toggleable: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HaRoom {
    pub name: String,
    pub entities: HaEntityGroups,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HaEntityGroups {
    pub devices: Vec<HaEntity>,
    pub scenes: Vec<HaEntity>,
    pub timers: Vec<HaEntity>,
    pub other: Vec<HaEntity>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WeatherForecast {
    pub condition: String,
    pub day: String,
    pub temperature: String,
}

#[derive(Clone, Debug)]
pub struct StateChange {
    pub entity_id: String,
    pub state: String,
    pub brightness: Option<u8>,
}

#[derive(Clone, Debug)]
pub enum HomeAssistantAction {
    StateChange(StateChange),
    ActivateScene(String),
    TimerAction { entity_id: String, action: String },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StateSnapshot {
    pub entities: Vec<HaEntity>,
    pub entity_groups: HaEntityGroups,
    pub rooms: Vec<HaRoom>,
    pub unassigned_entities: Vec<HaEntity>,
    pub unassigned_entity_groups: HaEntityGroups,
    pub weather_forecast: Vec<WeatherForecast>,
    pub weather_status: String,
    pub weather_temperature_unit: String,
    pub status: String,
}

#[derive(Debug, Default)]
struct State {
    entities: Vec<HaEntity>,
    pending_changes: VecDeque<HomeAssistantAction>,
    weather_forecast: Vec<WeatherForecast>,
    weather_status: String,
    weather_temperature_unit: String,
    status: String,
}

#[derive(Clone, Debug, Default)]
pub struct SharedState(Arc<Mutex<State>>);

impl SharedState {
    fn lock(&self) -> MutexGuard<'_, State> {
        match self.0.lock() {
            Ok(state) => state,
            Err(error) => {
                eprintln!("Home Assistant state lock was poisoned; recovering its contents");
                error.into_inner()
            }
        }
    }

    pub fn snapshot(&self) -> StateSnapshot {
        let state = self.lock();
        let mut rooms = std::collections::BTreeMap::<String, Vec<HaEntity>>::new();
        let mut unassigned_entities = Vec::new();
        for entity in &state.entities {
            if entity.area_name == "Unassigned" {
                unassigned_entities.push(entity.clone());
            } else {
                rooms.entry(entity.area_name.clone()).or_default().push(entity.clone());
            }
        }
        StateSnapshot {
            entities: state.entities.clone(),
            entity_groups: group_entities(&state.entities),
            rooms: rooms
                .into_iter()
                .map(|(name, entities)| HaRoom { name, entities: group_entities(&entities) })
                .collect(),
            unassigned_entity_groups: group_entities(&unassigned_entities),
            unassigned_entities,
            weather_forecast: state.weather_forecast.clone(),
            weather_status: state.weather_status.clone(),
            weather_temperature_unit: state.weather_temperature_unit.clone(),
            status: state.status.clone(),
        }
    }

    pub fn set_status(&self, status: impl Into<String>) {
        self.lock().status = status.into();
    }

    pub fn set_weather_forecast(
        &self,
        forecast: Vec<WeatherForecast>,
        temperature_unit: impl Into<String>,
    ) {
        let mut state = self.lock();
        state.weather_forecast = forecast;
        state.weather_temperature_unit = temperature_unit.into();
        state.weather_status = "Weather forecast updated".to_owned();
    }

    pub fn set_weather_status(&self, status: impl Into<String>) {
        let mut state = self.lock();
        state.weather_forecast.clear();
        state.weather_status = status.into();
    }

    pub fn set_entities(&self, mut entities: Vec<HaEntity>) {
        entities.sort_by(|left, right| {
            left.area_name
                .cmp(&right.area_name)
                .then_with(|| left.name.cmp(&right.name))
                .then_with(|| left.entity_id.cmp(&right.entity_id))
        });
        self.lock().entities = entities;
    }

    pub fn update_entity_state(&self, entity_id: &str, value: &str, brightness: Option<u8>) {
        let mut state = self.lock();
        if let Some(entity) = state.entities.iter_mut().find(|entity| entity.entity_id == entity_id)
        {
            entity.state = value.to_owned();
            if value == "off" {
                entity.brightness = 0;
            } else if let Some(brightness) = brightness {
                entity.brightness = brightness;
            } else if value == "on" && entity.brightness == 0 {
                entity.brightness = u8::MAX;
            }
        }
    }

    pub fn request_state_change(&self, entity_id: String, value: String) {
        self.lock().pending_changes.push_back(HomeAssistantAction::StateChange(StateChange {
            entity_id,
            state: value,
            brightness: None,
        }));
    }

    pub fn request_brightness_change(&self, entity_id: String, brightness: u8) {
        self.lock().pending_changes.push_back(HomeAssistantAction::StateChange(StateChange {
            entity_id,
            state: "on".to_owned(),
            brightness: Some(brightness),
        }));
    }

    pub fn request_scene_activation(&self, entity_id: String) {
        self.lock().pending_changes.push_back(HomeAssistantAction::ActivateScene(entity_id));
    }

    pub fn request_timer_action(&self, entity_id: String, action: String) {
        self.lock()
            .pending_changes
            .push_back(HomeAssistantAction::TimerAction { entity_id, action });
    }

    pub fn take_pending_changes(&self) -> Vec<HomeAssistantAction> {
        self.lock().pending_changes.drain(..).collect()
    }
}

fn group_entities(entities: &[HaEntity]) -> HaEntityGroups {
    let mut groups = HaEntityGroups::default();
    for entity in entities {
        match entity.domain.as_str() {
            "scene" => groups.scenes.push(entity.clone()),
            "timer" => groups.timers.push(entity.clone()),
            "light" | "switch" | "input_boolean" | "fan" | "media_player" => {
                groups.devices.push(entity.clone())
            }
            _ => groups.other.push(entity.clone()),
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::{HaEntity, HomeAssistantAction, SharedState};

    #[test]
    fn entity_state_updates_are_reflected_in_snapshots() {
        let state = SharedState::default();
        state.set_entities(vec![HaEntity {
            entity_id: "light.kitchen".into(),
            device_name: "Kitchen".into(),
            area_name: "Kitchen".into(),
            domain: "light".into(),
            name: "Ceiling".into(),
            state: "off".into(),
            brightness: 0,
            dimmable: true,
            toggleable: true,
        }]);

        state.update_entity_state("light.kitchen", "on", Some(180));

        assert_eq!(state.snapshot().entities[0].state, "on");
        assert_eq!(state.snapshot().entities[0].brightness, 180);
    }

    #[test]
    fn state_change_requests_are_consumed_once() {
        let state = SharedState::default();
        state.request_state_change("light.kitchen".into(), "on".into());

        let changes = state.take_pending_changes();
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            &changes[0],
            HomeAssistantAction::StateChange(change)
                if change.entity_id == "light.kitchen" && change.state == "on"
        ));
        assert!(state.take_pending_changes().is_empty());
    }

    #[test]
    fn snapshot_groups_scenes_and_timers_separately_from_devices() {
        let state = SharedState::default();
        state.set_entities(vec![
            HaEntity {
                entity_id: "scene.evening".into(),
                domain: "scene".into(),
                ..HaEntity::default()
            },
            HaEntity {
                entity_id: "timer.cooking".into(),
                domain: "timer".into(),
                ..HaEntity::default()
            },
            HaEntity {
                entity_id: "light.kitchen".into(),
                domain: "light".into(),
                ..HaEntity::default()
            },
        ]);

        let snapshot = state.snapshot();

        assert_eq!(snapshot.entity_groups.scenes[0].entity_id, "scene.evening");
        assert_eq!(snapshot.entity_groups.timers[0].entity_id, "timer.cooking");
        assert_eq!(snapshot.entity_groups.devices[0].entity_id, "light.kitchen");
    }

    #[test]
    fn snapshot_groups_entities_into_sorted_room_pages() {
        let state = SharedState::default();
        state.set_entities(vec![
            HaEntity {
                entity_id: "light.bedroom".into(),
                device_name: "Bedroom lamp".into(),
                area_name: "Bedroom".into(),
                domain: "light".into(),
                name: "Lamp".into(),
                state: "off".into(),
                brightness: 0,
                dimmable: true,
                toggleable: true,
            },
            HaEntity {
                entity_id: "light.kitchen".into(),
                device_name: "Kitchen lamp".into(),
                area_name: "Kitchen".into(),
                domain: "light".into(),
                name: "Lamp".into(),
                state: "off".into(),
                brightness: 0,
                dimmable: true,
                toggleable: true,
            },
        ]);

        let snapshot = state.snapshot();

        assert_eq!(snapshot.rooms.len(), 2);
        assert_eq!(snapshot.rooms[0].name, "Bedroom");
        assert_eq!(snapshot.rooms[1].name, "Kitchen");
        assert_eq!(snapshot.rooms[0].entities.devices[0].entity_id, "light.bedroom");
    }

    #[test]
    fn snapshot_keeps_unassigned_entities_on_their_own_page() {
        let state = SharedState::default();
        state.set_entities(vec![HaEntity {
            entity_id: "light.unassigned".into(),
            area_name: "Unassigned".into(),
            ..HaEntity::default()
        }]);

        let snapshot = state.snapshot();

        assert!(snapshot.rooms.is_empty());
        assert_eq!(snapshot.unassigned_entities.len(), 1);
        assert_eq!(snapshot.unassigned_entities[0].entity_id, "light.unassigned");
    }
}
