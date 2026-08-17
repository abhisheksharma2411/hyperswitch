use diesel::{Identifiable, Queryable, Selectable};

use crate::schema_v2::cluster_stats;

#[derive(
    Clone, Debug, Queryable, Identifiable, Selectable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = cluster_stats, check_for_backend(diesel::pg::Pg))]
#[diesel(primary_key(cluster_key))]
pub struct ClusterStats {
    pub cluster_key: String,
    pub statistics: serde_json::Value,
}

#[derive(Clone, Debug, diesel::Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = cluster_stats)]
pub struct ClusterStatsNew {
    pub cluster_key: String,
    pub statistics: serde_json::Value,
}
