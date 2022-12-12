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
