// Copyright © SixtyFPS GmbH <info@slint.dev>
// SPDX-License-Identifier: MIT

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HaEntity {
    pub entity_id: String,
    pub device_name: String,
    pub name: String,
    pub state: String,
    pub toggleable: bool,
}

#[derive(Clone, Debug)]
pub struct StateChange {
    pub entity_id: String,
    pub state: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StateSnapshot {
    pub entities: Vec<HaEntity>,
    pub status: String,
}

#[derive(Debug, Default)]
struct State {
    entities: Vec<HaEntity>,
    pending_changes: VecDeque<StateChange>,
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
        StateSnapshot { entities: state.entities.clone(), status: state.status.clone() }
    }

    pub fn set_status(&self, status: impl Into<String>) {
        self.lock().status = status.into();
    }

    pub fn set_entities(&self, entities: Vec<HaEntity>) {
        self.lock().entities = entities;
    }

    pub fn update_entity_state(&self, entity_id: &str, value: &str) {
        let mut state = self.lock();
        if let Some(entity) = state.entities.iter_mut().find(|entity| entity.entity_id == entity_id)
        {
            entity.state = value.to_owned();
        }
    }

    pub fn request_state_change(&self, entity_id: String, value: String) {
        self.lock().pending_changes.push_back(StateChange { entity_id, state: value });
    }

    pub fn take_pending_changes(&self) -> Vec<StateChange> {
        self.lock().pending_changes.drain(..).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{HaEntity, SharedState};

    #[test]
    fn entity_state_updates_are_reflected_in_snapshots() {
        let state = SharedState::default();
        state.set_entities(vec![HaEntity {
            entity_id: "light.kitchen".into(),
            device_name: "Kitchen".into(),
            name: "Ceiling".into(),
            state: "off".into(),
            toggleable: true,
        }]);

        state.update_entity_state("light.kitchen", "on");

        assert_eq!(state.snapshot().entities[0].state, "on");
    }

    #[test]
    fn state_change_requests_are_consumed_once() {
        let state = SharedState::default();
        state.request_state_change("light.kitchen".into(), "on".into());

        let changes = state.take_pending_changes();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].entity_id, "light.kitchen");
        assert_eq!(changes[0].state, "on");
        assert!(state.take_pending_changes().is_empty());
    }
}
