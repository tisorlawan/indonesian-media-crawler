use crate::{crawler::detik::DetikArticle, error::CrawlerError};
use chrono::{DateTime, FixedOffset};
use futures::TryStreamExt;
use sqlx::{sqlite::SqliteConnectOptions, Row, SqlitePool};
use tracing::debug;

enum Table {
    Queue(String),
    Result(String),
    Visited(String),
    Warned(String),
}
impl Table {
    fn get_name(&self) -> &str {
        match self {
            Table::Queue(name)
            | Table::Result(name)
            | Table::Visited(name)
            | Table::Warned(name) => name,
        }
    }
}

pub struct Persistent {
    pub name: String,
    queue_table: Table,
    visited_table: Table,
    warned_table: Table,
    result_table: Table,
    pool: SqlitePool,
}

impl Persistent {
    pub async fn new(name: &str) -> Result<Persistent, CrawlerError> {
        let opt = SqliteConnectOptions::new()
            .filename("db.sqlite3")
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(opt).await?;
        let p = Persistent {
            name: name.to_string(),
            queue_table: Table::Queue(format!("{}_queue", name)),
            visited_table: Table::Visited(format!("{}_visited", name)),
            warned_table: Table::Warned(format!("{}_warned", name)),
            result_table: Table::Result(format!("{}_results", name)),
            pool,
        };

        for table in &[
            &p.queue_table,
            &p.visited_table,
            &p.warned_table,
            &p.result_table,
        ] {
            if !p.is_table_exists(table).await? {
                p.create_table(table).await?;
            }
        }

        Ok(p)
    }

    async fn is_table_exists(&self, table: &Table) -> Result<bool, CrawlerError> {
        Ok(
            sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name = ?")
                .bind(table.get_name())
                .fetch_optional(&self.pool)
                .await?
                .is_some(),
        )
    }

    async fn create_table(&self, table: &Table) -> Result<(), CrawlerError> {
        match table {
            Table::Visited(t) | Table::Warned(t) | Table::Queue(t) => {
                let query = format!(
                    r#"
                        CREATE TABLE {} (
                            url TEXT PRIMARY KEY,
                            created_at DATETIME
                         )
                     "#,
                    t
                );
                sqlx::query(query.as_str()).execute(&self.pool).await?;
                debug!("Created {}", t);
                Ok(())
            }
            Table::Result(t) => {
                let query = format!(
                    r#"
                        CREATE TABLE {} (
                            url TEXT PRIMARY KEY,
                            created_at DATETIME,
                            title TEXT,
                            author TEXT,
                            published_date DATETIME,
                            description TEXT,
                            thumbnail_url TEXT,
                            keywords TEXT,
                            paragraphs TEXT
                        )
                    "#,
                    t
                );
                sqlx::query(query.as_str()).execute(&self.pool).await?;
                debug!("Created {}", t);
                Ok(())
            }
        }
    }

    async fn insert_url_timestamp<S: AsRef<str>>(
        &self,
        url: S,
        timestamp: DateTime<FixedOffset>,
        table: &Table,
    ) -> Result<(), CrawlerError> {
        let query = format!(
            "INSERT OR IGNORE INTO {} (url, created_at) VALUES (?, ?)",
            table.get_name()
        );
        sqlx::query(&query)
            .bind(url.as_ref())
            .bind(timestamp)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn insert_queue<S: AsRef<str>>(&self, url: S) -> Result<(), CrawlerError> {
        let timestamp = self.get_now();
        self.insert_url_timestamp(url, timestamp, &self.queue_table)
            .await
    }

    pub async fn insert_visited<S: AsRef<str>>(&self, url: S) -> Result<(), CrawlerError> {
        let timestamp = self.get_now();
        self.insert_url_timestamp(url, timestamp, &self.visited_table)
            .await
    }

    pub async fn insert_warned<S: AsRef<str>>(&self, url: S) -> Result<(), CrawlerError> {
        let timestamp = self.get_now();
        self.insert_url_timestamp(url, timestamp, &self.warned_table)
            .await
    }

    pub async fn insert_result<S: AsRef<str>>(
        &self,
        url: S,
        doc: DetikArticle,
    ) -> Result<(), CrawlerError> {
        let query = format!(
            r#"INSERT OR IGNORE INTO {} (
                url, 
                title, 
                published_date, 
                description, 
                thumbnail_url, 
                author, 
                keywords, 
                paragraphs, 
                created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            self.result_table.get_name()
        );
        sqlx::query(&query)
            .bind(url.as_ref().trim())
            .bind(doc.title)
            .bind(doc.published_date)
            .bind(doc.description)
            .bind(doc.thumbnail_url)
            .bind(doc.author)
            .bind(doc.keywords.join("|"))
            .bind(doc.paragraphs.join("\n"))
            .bind(self.get_now())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_by_url<S: AsRef<str>>(
        &self,
        url: S,
        table: &Table,
    ) -> Result<(), CrawlerError> {
        let query = format!(r#"DELETE FROM {} WHERE url = ?"#, table.get_name());
        sqlx::query(&query)
            .bind(url.as_ref())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_queue<S: AsRef<str>>(&self, url: S) -> Result<(), CrawlerError> {
        self.delete_by_url(url, &self.queue_table).await
    }

    pub async fn delete_result<S: AsRef<str>>(&self, url: S) -> Result<(), CrawlerError> {
        self.delete_by_url(url, &self.result_table).await
    }

    pub async fn delete_visited<S: AsRef<str>>(&self, url: S) -> Result<(), CrawlerError> {
        self.delete_by_url(url, &self.visited_table).await
    }

    pub async fn delete_warned<S: AsRef<str>>(&self, url: S) -> Result<(), CrawlerError> {
        self.delete_by_url(url, &self.warned_table).await
    }

    pub async fn get_queue(&self) -> Result<Vec<String>, CrawlerError> {
        let mut urls: Vec<String> = vec![];
        let query = format!("SELECT url FROM {}", self.queue_table.get_name());
        let mut rows = sqlx::query(&query).fetch(&self.pool);
        while let Some(row) = rows.try_next().await? {
            urls.push(row.try_get("url")?);
        }

        Ok(urls)
    }

    pub async fn is_visited<S: AsRef<str>>(&self, url: S) -> Result<bool, CrawlerError> {
        let query = format!(
            "SELECT url FROM {} WHERE url = ?",
            self.visited_table.get_name()
        );

        Ok(sqlx::query(&query)
            .bind(url.as_ref().trim())
            .fetch_optional(&self.pool)
            .await?
            .is_some())
    }

    fn get_now(&self) -> DateTime<FixedOffset> {
        DateTime::parse_from_rfc3339(
            &chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        )
        .unwrap()
    }
}
