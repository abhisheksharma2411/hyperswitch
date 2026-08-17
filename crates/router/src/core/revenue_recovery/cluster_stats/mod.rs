pub mod document;
pub mod events;
pub mod keys;
pub mod record;
pub mod stub;

use hyperswitch_domain_models::payments::payment_attempt::PaymentAttempt;

use crate::{routes::SessionState, services::logger};

pub async fn record_outcome_from_attempt(
    state: &SessionState,
    payment_attempt: &PaymentAttempt,
    revenue_recovery_metadata: &api_models::payments::PaymentRevenueRecoveryMetadata,
    success: bool,
) {
    if !state.conf.revenue_recovery.enable_retry_stats_logging {
        return;
    }
    let event = events::build_outcome_event(payment_attempt, revenue_recovery_metadata, success);
    record::record_outcome(&event).await;
    logger::info!(
        cluster_key = %event.key.as_db(),
        success,
        "cluster_stats outcome recorded (PoC log-only)"
    );
}
