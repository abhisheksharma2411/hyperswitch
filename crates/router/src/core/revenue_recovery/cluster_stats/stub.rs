use time::OffsetDateTime;

use super::{document::StatsDocument, keys::ClusterKey};

#[derive(Clone, Debug)]
pub struct RetryWindow {
    pub earliest: OffsetDateTime,
    pub latest: OffsetDateTime,
    pub remaining_today: u32,
    pub remaining_month: u32,
}

// MathModel body owned by another dev; returns None until filled in.
#[router_env::instrument(skip_all)]
pub fn compute_mathmodel_retry_time(
    _key: &ClusterKey,
    _doc: &StatsDocument,
    _window: &RetryWindow,
) -> Option<OffsetDateTime> {
    None
}
