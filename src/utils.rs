use chrono::{DateTime, FixedOffset};
use sqlx::SqlitePool;

pub(crate) async fn is_table_exists(
    pool: &SqlitePool,
    table_name: &str,
) -> Result<bool, sqlx::Error> {
    Ok(
        sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name = ?")
            .bind(table_name)
            .fetch_optional(pool)
            .await?
            .is_some(),
    )
}

pub(crate) fn get_now() -> DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339(
        &chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    )
    .unwrap()
}
