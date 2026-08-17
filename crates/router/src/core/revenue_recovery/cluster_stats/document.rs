use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::PrimitiveDateTime;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotCounter {
    #[serde(default)]
    pub n: u64,
    #[serde(default)]
    pub k: u64,
}

impl SlotCounter {
    pub fn record(&mut self, success: bool) {
        self.n = self.n.saturating_add(1);
        if success {
            self.k = self.k.saturating_add(1);
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsDocument {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub dow: BTreeMap<String, SlotCounter>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub dom: BTreeMap<String, SlotCounter>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hod: BTreeMap<String, SlotCounter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_at: Option<String>,
}

impl StatsDocument {
    pub fn from_json(value: &serde_json::Value) -> Result<Self, serde_json::Error> {
        serde_json::from_value(value.clone())
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_else(|_| serde_json::json!({}))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventSlots {
    pub dow: u8,
    pub dom: u8,
    pub hod: u8,
}

impl EventSlots {
    pub fn from_primitive(ts: time::PrimitiveDateTime) -> Self {
        Self {
            dow: ts.weekday().number_days_from_monday(),
            dom: ts.day().saturating_sub(1),
            hod: ts.hour(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SlotUpdate {
    pub slot: u8,
    pub success: bool,
}

#[derive(Clone, Debug)]
pub struct StatsDelta {
    pub updates: Vec<(SlotFamily, SlotUpdate)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SlotFamily {
    Dow,
    Dom,
    Hod,
}

impl SlotFamily {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Dow => "dow",
            Self::Dom => "dom",
            Self::Hod => "hod",
        }
    }
}

pub fn merge_stats(current: Option<&StatsDocument>, delta: &StatsDelta) -> StatsDocument {
    let mut doc = current.cloned().unwrap_or_default();
    for (family, update) in &delta.updates {
        let map = match family {
            SlotFamily::Dow => &mut doc.dow,
            SlotFamily::Dom => &mut doc.dom,
            SlotFamily::Hod => &mut doc.hod,
        };
        map.entry(update.slot.to_string())
            .or_default()
            .record(update.success);
    }
    doc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delta(slots: &[(SlotFamily, u8, bool)]) -> StatsDelta {
        StatsDelta {
            updates: slots
                .iter()
                .map(|(f, s, ok)| (*f, SlotUpdate { slot: *s, success: *ok }))
                .collect(),
        }
    }

    #[test]
    fn merge_into_empty_creates_entries() {
        let doc = merge_stats(None, &delta(&[(SlotFamily::Dow, 3, true)]));
        assert_eq!(doc.dow["3"], SlotCounter { n: 1, k: 1 });
        assert!(doc.dom.is_empty());
        assert!(doc.hod.is_empty());
    }

    #[test]
    fn merge_accumulates_across_calls() {
        let d1 = delta(&[(SlotFamily::Hod, 9, true)]);
        let d2 = delta(&[(SlotFamily::Hod, 9, false)]);
        let d3 = delta(&[(SlotFamily::Hod, 10, true)]);
        let doc = merge_stats(Some(&merge_stats(None, &d1)), &d2);
        let doc = merge_stats(Some(&doc), &d3);
        assert_eq!(doc.hod["9"], SlotCounter { n: 2, k: 1 });
        assert_eq!(doc.hod["10"], SlotCounter { n: 1, k: 1 });
    }

    #[test]
    fn merge_is_associative_over_deltas() {
        let d1 = delta(&[(SlotFamily::Dom, 9, true)]);
        let d2 = delta(&[(SlotFamily::Dom, 9, false)]);
        let d3 = delta(&[(SlotFamily::Dom, 9, true)]);
        let left = merge_stats(Some(&merge_stats(Some(&merge_stats(None, &d1)), &d2)), &d3);
        let right = merge_stats(
            Some(&merge_stats(None, &d1)),
            &merge_stats(Some(&StatsDocument::default()), &d2).into(),
        );
        let mut combined = StatsDelta { updates: vec![] };
        combined.updates.extend(d2.updates.clone());
        combined.updates.extend(d3.updates.clone());
        let right = merge_stats(Some(&merge_stats(None, &d1)), &combined);
        assert_eq!(left.dom["9"], right.dom["9"]);
    }

    #[test]
    fn marginal_consistency_holds() {
        let d = delta(&[
            (SlotFamily::Dow, 1, true),
            (SlotFamily::Dom, 5, true),
            (SlotFamily::Hod, 9, true),
        ]);
        let doc = merge_stats(None, &d);
        let sum = |m: &BTreeMap<String, SlotCounter>| m.values().map(|c| c.n).sum::<u64>();
        assert_eq!(sum(&doc.dow), sum(&doc.dom));
        assert_eq!(sum(&doc.dom), sum(&doc.hod));
    }
}
