#[derive(Debug, thiserror::Error)]
pub enum CrawlerError {
    #[error("Database error")]
    DatabaseError(#[from] sqlx::error::Error),
}
