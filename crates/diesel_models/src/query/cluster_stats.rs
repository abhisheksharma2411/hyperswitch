use diesel::{associations::HasTable, ExpressionMethods, QueryDsl, RunQueryDsl};
use error_stack::ResultExt;

use crate::{
    cluster_stats::{ClusterStats, ClusterStatsNew},
    errors, schema_v2::cluster_stats::dsl, PgPooledConn, StorageResult,
};

impl ClusterStatsNew {
    pub async fn insert_or_replace(self, conn: &PgPooledConn) -> StorageResult<ClusterStats> {
        diesel::insert_into(<Self as HasTable>::table())
            .values(&self)
            .on_conflict(dsl::cluster_key)
            .do_update()
            .set(dsl::statistics.eq(diesel::upsert::excluded(dsl::statistics)))
            .get_result::<ClusterStats>(conn)
            .await
            .change_context(errors::DatabaseError::Others)
            .attach_printable("error upserting cluster_stats row")
    }
}

impl ClusterStats {
    pub async fn find_by_keys(
        conn: &PgPooledConn,
        keys: Vec<String>,
    ) -> StorageResult<Vec<ClusterStats>> {
        <Self as HasTable>::table()
            .filter(dsl::cluster_key.eq_any(keys))
            .get_results::<ClusterStats>(conn)
            .await
            .change_context(errors::DatabaseError::Others)
            .attach_printable("error fetching cluster_stats rows by keys")
    }

    pub async fn find_by_key_prefix(
        conn: &PgPooledConn,
        key_prefix: &str,
    ) -> StorageResult<Vec<ClusterStats>> {
        <Self as HasTable>::table()
            .filter(dsl::cluster_key.like(format!("{key_prefix}%")))
            .get_results::<ClusterStats>(conn)
            .await
            .change_context(errors::DatabaseError::Others)
            .attach_printable("error scanning cluster_stats rows by key prefix")
    }
}
