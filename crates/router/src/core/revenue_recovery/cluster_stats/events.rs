use api_models::payments::PaymentRevenueRecoveryMetadata;
use hyperswitch_domain_models::payments::payment_attempt::PaymentAttempt;

use super::{
    document::{EventSlots, StatsDelta},
    keys::{ClusterKey, Dim},
};

pub struct RetryOutcomeEvent {
    pub key: ClusterKey,
    pub delta: StatsDelta,
    pub success: bool,
}

fn error_code_dim(payment_attempt: &PaymentAttempt) -> Dim {
    payment_attempt
        .error
        .as_ref()
        .and_then(|details| details.unified_code.as_ref().or(Some(&details.code)))
        .map(|code| Dim::from_event_value(Some(code.as_str())))
        .unwrap_or(Dim::Unknown)
}

pub fn build_outcome_event(
    payment_attempt: &PaymentAttempt,
    revenue_recovery_metadata: &PaymentRevenueRecoveryMetadata,
    success: bool,
) -> RetryOutcomeEvent {
    let error_code_dim = error_code_dim(payment_attempt);

    let card_details = revenue_recovery_metadata
        .billing_connector_payment_method_details
        .as_ref()
        .and_then(|details| details.get_billing_connector_card_info());
    let card_type_dim = Dim::from_event_value(
        card_details
            .and_then(|card| card.card_network)
            .map(|network| network.to_string())
            .as_deref(),
    );
    let issuer_dim = Dim::from_event_value(card_details.and_then(|card| card.card_issuer.as_deref()));

    let slots = EventSlots::at(payment_attempt.created_at);
    let delta = StatsDelta::for_event(slots, success);

    RetryOutcomeEvent {
        key: ClusterKey::leaf(error_code_dim, card_type_dim, issuer_dim),
        delta,
        success,
    }
}
