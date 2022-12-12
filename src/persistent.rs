use crate::db_utils;
use crate::{crawler::detik::DetikArticle, error::CrawlerError};
use chrono::{DateTime, FixedOffset};
use sqlx::{sqlite::SqliteConnectOptions, Row, SqlitePool};
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

    async fn count(&self) -> Result<u32, CrawlerError> {
        let query = format!("SELECT COUNT(*) FROM {}", self.get_name());
        Ok(sqlx::query(&query)
            .fetch_one(self.get_pool())
            .await?
            .try_get(0)?)
    }
}

pub struct UrlTable {
    name: String,
    pool: SqlitePool,
}

#[async_trait::async_trait]
impl Table for UrlTable {
    type Record<'a> = &'a str where Self: 'a;

    fn get_name(&self) -> &str {
        self.name.as_str()
    }

    fn get_pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn create(&self) -> Result<(), sqlx::Error> {
        if !db_utils::is_table_exists(self.get_pool(), &self.name).await? {
            let query = format!(
                r#"
                    CREATE TABLE {} (
                        id TEXT PRIMARY KEY,
                        created_at DATETIME
                    )
                    "#,
                &self.name
            );
            sqlx::query(query.as_str()).execute(self.get_pool()).await?;
        }
        Ok(())
    }

    async fn insert<'a>(&self, record: Self::Record<'a>) -> Result<(), sqlx::Error> {
        let timestamp = get_now();
        let mut tx = self.get_pool().begin().await?;
        let query = format!(
            "INSERT OR IGNORE INTO {} (id, created_at) VALUES (?, ?)",
            &self.name
        );
        sqlx::query(&query)
            .bind(record)
            .bind(timestamp)
            .execute(&mut tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

pub struct ArticleTable {
    name: String,
    pool: SqlitePool,
}

#[async_trait::async_trait]
impl Table for ArticleTable {
    type Record<'a> = (&'a str, DetikArticle);

    fn get_name(&self) -> &str {
        self.name.as_str()
    }

    fn get_pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn create(&self) -> Result<(), sqlx::Error> {
        if !db_utils::is_table_exists(self.get_pool(), &self.name).await? {
            let query = format!(
                r#"
                        CREATE TABLE {} (
                            id TEXT PRIMARY KEY,
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
                &self.name
            );
            sqlx::query(query.as_str()).execute(self.get_pool()).await?;
        }
        Ok(())
    }

    async fn insert<'a>(&self, (url, doc): Self::Record<'a>) -> Result<(), sqlx::Error> {
        let mut tx = self.get_pool().begin().await?;
        let query = format!(
            r#"INSERT OR IGNORE INTO {} (
                id, 
                title, 
                published_date, 
                description, 
                thumbnail_url, 
                author, 
                keywords, 
                paragraphs, 
                created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            self.name
        );
        sqlx::query(&query)
            .bind(url.trim())
            .bind(doc.title)
            .bind(doc.published_date)
            .bind(doc.description)
            .bind(doc.thumbnail_url)
            .bind(doc.author)
            .bind(doc.keywords.join("|"))
            .bind(doc.paragraphs.join("\n"))
            .bind(get_now())
            .execute(&mut tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

pub struct Persistent {
    pub name: String,
    pub queued: UrlTable,
    pub visited: UrlTable,
    pub warned: UrlTable,
    pub results: ArticleTable,
    pub running: UrlTable,
    pool: SqlitePool,
}

impl Persistent {
    pub async fn new(name: &str) -> Result<Persistent, CrawlerError> {
        let opt = SqliteConnectOptions::new()
            .filename(format!("{}.db", name))
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(opt).await?;
        let p = Persistent {
            name: name.to_string(),
            queued: UrlTable {
                name: format!("{}_queued", name),
                pool: pool.clone(),
            },
            running: UrlTable {
                name: format!("{}_running", name),
                pool: pool.clone(),
            },
            visited: UrlTable {
                name: format!("{}_visited", name),
                pool: pool.clone(),
            },
            warned: UrlTable {
                name: format!("{}_warned", name),
                pool: pool.clone(),
            },
            results: ArticleTable {
                name: format!("{}_results", name),
                pool: pool.clone(),
            },
            pool,
        };

        for table in &[&p.queued, &p.running, &p.visited, &p.warned] {
            if !db_utils::is_table_exists(&p.pool, &table.name).await? {
                tracing::debug!("Crate table {}", table.name);
                table.create().await?;
            } else {
                tracing::debug!("Use table {}", table.name);
            }
        }
        if !db_utils::is_table_exists(&p.pool, &p.results.name).await? {
            tracing::debug!("Crate table {}", p.results.name);
            p.results.create().await?;
        } else {
            tracing::debug!("Use table {}", p.results.name);
        }

        Ok(p)
    }

    pub async fn get_queue(&self) -> Result<Vec<String>, CrawlerError> {
        let mut urls: Vec<String> = vec![];

        // Get queue
        let query = format!("SELECT id FROM {} ORDER BY created_at", self.queued.name);
        for row in sqlx::query(&query).fetch_all(&self.pool).await? {
            urls.push(row.try_get("id")?);
        }

        Ok(urls)
    }

    pub async fn get_running(&self) -> Result<Vec<String>, CrawlerError> {
        let mut in_progress: Vec<String> = vec![];
        let query = format!(
            "SELECT id FROM {} ORDER BY created_at",
            self.running.get_name()
        );
        for row in sqlx::query(&query).fetch_all(&self.pool).await? {
            in_progress.push(row.try_get("id")?);
        }
        Ok(in_progress)
    }

    pub async fn get_queue_n(&self, n: u32) -> Result<Vec<String>, CrawlerError> {
        let mut in_progress: Vec<String> = vec![];
        let query = format!(
            "SELECT id FROM {} ORDER BY created_at LIMIT ?",
            self.queued.get_name()
        );
        for row in sqlx::query(&query).bind(n).fetch_all(&self.pool).await? {
            in_progress.push(row.try_get("id")?);
        }
        Ok(in_progress)
    }

    pub async fn merge_queue_and_running(&self) -> Result<(), CrawlerError> {
        let in_progress = self.get_running().await?;
        for i in in_progress {
            self.queued.insert(i.as_str()).await?;
            self.running.delete(i.as_str()).await?;
        }
        Ok(())
    }
}

fn get_now() -> DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339(
        &chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crawler::detik::DetikArticle;
    use pretty_assertions::assert_eq;
    use std::path::Path;
    use tokio::fs;

    #[tokio::test]
    async fn create_new_file() {
        if Path::new("test.db").is_file() {
            fs::remove_file("test.db").await.unwrap();
        }

        assert!(!Path::new("test.db").is_file());
        Persistent::new("test").await.unwrap();
        assert!(Path::new("test.db").is_file());

        fs::remove_file("test.db").await.unwrap();
    }

    #[tokio::test]
    async fn create_and_delete_rows() {
        if Path::new("test2.db").is_file() {
            fs::remove_file("test2.db").await.unwrap();
        }

        let p = Persistent::new("test2").await.unwrap();

        assert_eq!(p.visited.count().await.unwrap(), 0);
        assert!(!p.visited.is_exist("visited").await.unwrap());
        p.visited.insert("visited").await.unwrap();
        assert_eq!(p.visited.count().await.unwrap(), 1);
        assert!(p.visited.is_exist("visited").await.unwrap());
        p.visited.delete("visited").await.unwrap();
        assert_eq!(p.visited.count().await.unwrap(), 0);
        assert!(!p.visited.is_exist("visited").await.unwrap());

        assert_eq!(p.queued.count().await.unwrap(), 0);
        assert!(!p.queued.is_exist("queued").await.unwrap());
        p.queued.insert("queued").await.unwrap();
        assert_eq!(p.queued.count().await.unwrap(), 1);
        assert!(p.queued.is_exist("queued").await.unwrap());
        p.queued.delete("queued").await.unwrap();
        assert_eq!(p.queued.count().await.unwrap(), 0);
        assert!(!p.queued.is_exist("queued").await.unwrap());

        assert_eq!(p.running.count().await.unwrap(), 0);
        assert!(!p.running.is_exist("running").await.unwrap());
        p.running.insert("running").await.unwrap();
        assert_eq!(p.running.count().await.unwrap(), 1);
        assert!(p.running.is_exist("running").await.unwrap());
        p.running.delete("running").await.unwrap();
        assert_eq!(p.running.count().await.unwrap(), 0);
        assert!(!p.running.is_exist("running").await.unwrap());

        assert_eq!(p.warned.count().await.unwrap(), 0);
        assert!(!p.warned.is_exist("warned").await.unwrap());
        p.warned.insert("warned").await.unwrap();
        assert_eq!(p.warned.count().await.unwrap(), 1);
        assert!(p.warned.is_exist("warned").await.unwrap());
        p.warned.delete("warned").await.unwrap();
        assert_eq!(p.warned.count().await.unwrap(), 0);
        assert!(!p.warned.is_exist("warned").await.unwrap());

        let d = DetikArticle {
            author: Some("author".into()),
            description: Some("description".into()),
            keywords: vec!["k1".to_string(), "k2".to_string()],
            paragraphs: vec!["p1".to_string(), "p2".to_string()],
            published_date: Some(get_now()),
            thumbnail_url: None,
            title: Some("title".to_string()),
        };

        assert_eq!(p.results.count().await.unwrap(), 0);
        assert!(!p.results.is_exist("results").await.unwrap());
        p.results.insert(("results", d)).await.unwrap();
        assert_eq!(p.results.count().await.unwrap(), 1);
        assert!(p.results.is_exist("results").await.unwrap());
        p.results.delete("results").await.unwrap();
        assert_eq!(p.results.count().await.unwrap(), 0);
        assert!(!p.results.is_exist("results").await.unwrap());

        fs::remove_file("test2.db").await.unwrap();
    }

    #[tokio::test]
    async fn get_queue() {
        if Path::new("test3.db").is_file() {
            fs::remove_file("test3.db").await.unwrap();
        }

        let p = Persistent::new("test3").await.unwrap();
        let queue: Vec<String> = vec![];
        assert_eq!(p.get_queue().await.unwrap(), queue);

        p.queued.insert("1").await.unwrap();
        p.queued.insert("2").await.unwrap();
        p.queued.insert("3").await.unwrap();
        let queue: Vec<String> = vec!["1", "2", "3"]
            .into_iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(p.queued.count().await.unwrap(), 3);
        assert_eq!(p.get_queue().await.unwrap(), queue);

        p.queued.delete("2").await.unwrap();
        let queue: Vec<String> = vec!["1", "3"]
            .into_iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(p.get_queue().await.unwrap(), queue);

        p.queued.delete("1").await.unwrap();
        let queue: Vec<String> = vec!["3"].into_iter().map(ToString::to_string).collect();
        assert_eq!(p.get_queue().await.unwrap(), queue);

        p.queued.insert("1").await.unwrap();
        let queue: Vec<String> = vec!["3", "1"]
            .into_iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(p.get_queue().await.unwrap(), queue);

        p.queued.delete("3").await.unwrap();
        let queue: Vec<String> = vec!["1"].into_iter().map(ToString::to_string).collect();
        assert_eq!(p.get_queue().await.unwrap(), queue);

        p.queued.delete("2").await.unwrap();
        let queue: Vec<String> = vec!["1"].into_iter().map(ToString::to_string).collect();
        assert_eq!(p.get_queue().await.unwrap(), queue);

        p.queued.delete("1").await.unwrap();
        let queue: Vec<String> = vec![];
        assert_eq!(p.get_queue().await.unwrap(), queue);

        fs::remove_file("test3.db").await.unwrap();
    }

    #[tokio::test]
    async fn get_queue_n() {
        if Path::new("test4.db").is_file() {
            fs::remove_file("test4.db").await.unwrap();
        }
        let p = Persistent::new("test4").await.unwrap();

        p.queued.insert("1").await.unwrap();
        p.queued.insert("2").await.unwrap();
        p.queued.insert("3").await.unwrap();
        p.queued.insert("4").await.unwrap();
        p.queued.insert("5").await.unwrap();

        let queue_2: Vec<String> = vec!["1", "2"]
            .into_iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(p.get_queue_n(2).await.unwrap(), queue_2);

        fs::remove_file("test4.db").await.unwrap();
    }

    #[tokio::test]
    async fn get_running() {
        if Path::new("test5.db").is_file() {
            fs::remove_file("test5.db").await.unwrap();
        }
        let p = Persistent::new("test5").await.unwrap();

        let running: Vec<String> = vec![];
        assert_eq!(p.get_running().await.unwrap(), running);

        p.running.insert("1").await.unwrap();
        p.running.insert("2").await.unwrap();
        p.running.insert("3").await.unwrap();
        let running: Vec<String> = vec!["1", "2", "3"]
            .into_iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(p.running.count().await.unwrap(), 3);
        assert_eq!(p.get_running().await.unwrap(), running);

        p.running.delete("2").await.unwrap();
        let running: Vec<String> = vec!["1", "3"]
            .into_iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(p.get_running().await.unwrap(), running);

        p.running.delete("1").await.unwrap();
        let running: Vec<String> = vec!["3"].into_iter().map(ToString::to_string).collect();
        assert_eq!(p.get_running().await.unwrap(), running);

        p.running.insert("1").await.unwrap();
        let running: Vec<String> = vec!["3", "1"]
            .into_iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(p.get_running().await.unwrap(), running);

        p.running.delete("3").await.unwrap();
        let running: Vec<String> = vec!["1"].into_iter().map(ToString::to_string).collect();
        assert_eq!(p.get_running().await.unwrap(), running);

        p.running.delete("2").await.unwrap();
        let running: Vec<String> = vec!["1"].into_iter().map(ToString::to_string).collect();
        assert_eq!(p.get_running().await.unwrap(), running);

        p.running.delete("1").await.unwrap();
        let running: Vec<String> = vec![];
        assert_eq!(p.get_running().await.unwrap(), running);

        fs::remove_file("test5.db").await.unwrap();
    }

    #[tokio::test]
    async fn merge_queue_and_running() {
        if Path::new("test6.db").is_file() {
            fs::remove_file("test6.db").await.unwrap();
        }
        let p = Persistent::new("test6").await.unwrap();

        p.queued.insert("1").await.unwrap();
        p.queued.insert("2").await.unwrap();
        p.queued.insert("3").await.unwrap();
        p.running.insert("4").await.unwrap();
        p.running.insert("5").await.unwrap();

        p.merge_queue_and_running().await.unwrap();

        let queue: Vec<String> = vec!["1", "2", "3", "4", "5"]
            .into_iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(p.get_queue().await.unwrap(), queue);

        let running: Vec<String> = vec![];
        assert_eq!(p.get_running().await.unwrap(), running);

        fs::remove_file("test6.db").await.unwrap();
    }
}
