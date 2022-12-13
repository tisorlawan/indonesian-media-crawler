use sqlx::{Row, SqlitePool};
use std::fmt::Display;

#[async_trait::async_trait]
pub trait Table {
    type Record<'a>;

    fn get_name(&self) -> &str;
    fn get_pool(&self) -> &SqlitePool;

    async fn create(&self) -> Result<(), sqlx::Error>;
    async fn insert<'a>(&self, record: Self::Record<'a>) -> Result<(), sqlx::Error>;

    async fn is_exist<I: AsRef<str> + Display + Send + Sync>(
        &self,
        id: I,
    ) -> Result<bool, sqlx::Error> {
        let query = format!("SELECT id FROM {} WHERE id = ?", self.get_name());
        Ok(sqlx::query(&query)
            .bind(id.as_ref())
            .fetch_optional(self.get_pool())
            .await?
            .is_some())
    }

    async fn delete<I: AsRef<str> + Send + Sync>(&self, id: I) -> Result<(), sqlx::Error> {
        let query = format!(r#"DELETE FROM {} WHERE id = ?"#, self.get_name());
        sqlx::query(&query)
            .bind(id.as_ref())
            .execute(self.get_pool())
            .await?;
        Ok(())
    }

    async fn count(&self) -> Result<u32, sqlx::Error> {
        let query = format!("SELECT COUNT(*) FROM {}", self.get_name());
        Ok(sqlx::query(&query)
            .fetch_one(self.get_pool())
            .await?
            .try_get(0)?)
    }
}
