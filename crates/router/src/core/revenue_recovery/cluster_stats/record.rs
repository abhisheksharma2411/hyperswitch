use router_env::{instrument, tracing};

use super::{document::SlotFamily, events::RetryOutcomeEvent};

#[instrument(skip_all)]
pub async fn record_outcome(event: &RetryOutcomeEvent) {
    let (dow, dom, hod) = event.delta.updates.iter().fold(
        (None, None, None),
        |(mut d, mut m, mut h), (family, update)| {
            match family {
                SlotFamily::Dow => d = Some(update.slot),
                SlotFamily::Dom => m = Some(update.slot),
                SlotFamily::Hod => h = Some(update.slot),
            }
            (d, m, h)
        },
    );

    // Stub boundary: log-only until storage_impl persistence is wired in the next iteration.
    tracing::info!(
        cluster_key = %event.key.as_db(),
        depth = ?event.key.depth(),
        dow_slot = dow,
        dom_slot = dom,
        hod_slot = hod,
        success = event.success,
        "cluster_stats retry outcome (log-only, no persistence yet)"
    );
}
