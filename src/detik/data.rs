use super::DetikArticle;
use crate::{utils, CrawlerError, Storage, Table};
use sqlx::{sqlite::SqliteConnectOptions, Row, SqlitePool};

pub struct UrlTable {
    name: String,
    pool: SqlitePool,
}

#[async_trait::async_trait]
impl Table for UrlTable {
    type Record<'a> = &'a str;

    fn get_name(&self) -> &str {
        self.name.as_str()
    }

    fn get_pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn create(&self) -> Result<(), sqlx::Error> {
        if !utils::is_table_exists(self.get_pool(), &self.name).await? {
            let query = format!(
                "CREATE TABLE {} (
                    id TEXT PRIMARY KEY,
                    created_at DATETIME
                 )",
                &self.name
            );
            sqlx::query(query.as_str()).execute(self.get_pool()).await?;
        }
        Ok(())
    }

    async fn insert<'a>(&self, record: Self::Record<'a>) -> Result<(), sqlx::Error> {
        let timestamp = utils::get_now();
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

pub struct DetikArticleTable {
    name: String,
    pool: SqlitePool,
}

#[async_trait::async_trait]
impl Table for DetikArticleTable {
    type Record<'a> = (&'a str, DetikArticle);

    fn get_name(&self) -> &str {
        self.name.as_str()
    }

    fn get_pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn create(&self) -> Result<(), sqlx::Error> {
        if !utils::is_table_exists(self.get_pool(), &self.name).await? {
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

    async fn insert<'a>(&self, (url, record): Self::Record<'a>) -> Result<(), sqlx::Error> {
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
            .bind(record.title)
            .bind(record.published_date)
            .bind(record.description)
            .bind(record.thumbnail_url)
            .bind(record.author)
            .bind(record.keywords.join("|"))
            .bind(record.paragraphs.join("\n"))
            .bind(utils::get_now())
            .execute(&mut tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

pub struct DetikData {
    pub name: String,
    pub queued: UrlTable,
    pub visited: UrlTable,
    pub warned: UrlTable,
    pub results: DetikArticleTable,
    pub running: UrlTable,
    pool: SqlitePool,
}

impl DetikData {
    pub async fn new(name: &str) -> Result<DetikData, CrawlerError> {
        let opt = SqliteConnectOptions::new()
            .filename(format!("{}.db", name))
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(opt).await?;
        let p = DetikData {
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
            results: DetikArticleTable {
                name: format!("{}_results", name),
                pool: pool.clone(),
            },
            pool,
        };

        for table in &[&p.queued, &p.running, &p.visited, &p.warned] {
            if !utils::is_table_exists(&p.pool, &table.name).await? {
                tracing::debug!("Crate table {}", table.name);
                table.create().await?;
            } else {
                tracing::debug!("Use table {}", table.name);
            }
        }
        if !utils::is_table_exists(&p.pool, &p.results.name).await? {
            tracing::debug!("Crate table {}", p.results.name);
            p.results.create().await?;
        } else {
            tracing::debug!("Use table {}", p.results.name);
        }

        Ok(p)
    }
}

#[async_trait::async_trait]
impl Storage for DetikData {
    type Record = DetikArticle;

    async fn queued_get(&self) -> Result<Vec<String>, CrawlerError> {
        let mut urls: Vec<String> = vec![];

        // Get queue
        let query = format!("SELECT id FROM {} ORDER BY created_at", self.queued.name);
        for row in sqlx::query(&query).fetch_all(&self.pool).await? {
            urls.push(row.try_get("id")?);
        }

        Ok(urls)
    }

    async fn queued_get_n(&self, n: u32) -> Result<Vec<String>, CrawlerError> {
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

    async fn queued_insert<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError> {
        let item = item.as_ref();
        Ok(self.queued.insert(item).await?)
    }

    async fn queued_delete<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError> {
        let item = item.as_ref();
        Ok(self.queued.delete(item).await?)
    }

    async fn queued_is_exists<I: AsRef<str> + Send>(&self, item: I) -> Result<bool, CrawlerError> {
        let item = item.as_ref();
        Ok(self.queued.is_exist(item).await?)
    }

    async fn running_get(&self) -> Result<Vec<String>, CrawlerError> {
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

    async fn running_insert<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError> {
        let item = item.as_ref();
        Ok(self.running.insert(item.as_ref()).await?)
    }

    async fn running_delete<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError> {
        let item = item.as_ref();
        Ok(self.running.delete(item).await?)
    }

    async fn running_count(&self) -> Result<u32, CrawlerError> {
        Ok(self.running.count().await?)
    }

    async fn running_is_exists<I: AsRef<str> + Send>(&self, item: I) -> Result<bool, CrawlerError> {
        let item = item.as_ref();
        Ok(self.running.is_exist(item).await?)
    }

    async fn visited_delete<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError> {
        let item = item.as_ref();
        Ok(self.visited.delete(item).await?)
    }

    async fn visited_is_exists<I: AsRef<str> + Send>(&self, item: I) -> Result<bool, CrawlerError> {
        let item = item.as_ref();
        Ok(self.visited.is_exist(item).await?)
    }

    async fn visited_insert<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError> {
        let item = item.as_ref();
        Ok(self.visited.insert(item).await?)
    }

    async fn results_count(&self) -> Result<u32, CrawlerError> {
        Ok(self.results.count().await?)
    }

    async fn results_insert<I: AsRef<str> + Send>(
        &self,
        (url, record): (I, Self::Record),
    ) -> Result<(), CrawlerError> {
        let url = url.as_ref();
        Ok(self.results.insert((url, record)).await?)
    }

    async fn warned_insert<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError> {
        let item = item.as_ref();
        Ok(self.warned.insert(item).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::super::DetikArticle;
    use super::*;
    use crate::utils::get_now;
    use std::path::Path;
    use tokio::fs;

    macro_rules! insert {
        ($it:expr, $($added:expr),+) => {
            $(
                $it.insert($added).await.unwrap();
            )+
        };
    }

    macro_rules! delete {
        ($it:expr, $($deleted:expr),+) => {
            $(
                $it.delete($deleted).await.unwrap();
            )+
        }
    }

    macro_rules! assert_eq_fut_strings {
        ($fut:expr, $($item:expr),+) => {
            let mut v = Vec::new();
            $(
                v.push($item.to_string());
            )+
            assert_eq!($fut.await.unwrap(), v);
        };
        ($fut:expr) => {
            let v: Vec<String> = vec![];
            assert_eq!($fut.await.unwrap(), v);
        }
    }

    #[tokio::test]
    async fn create_new_file() {
        if Path::new("test.db").is_file() {
            fs::remove_file("test.db").await.unwrap();
        }

        assert!(!Path::new("test.db").is_file());
        DetikData::new("test").await.unwrap();
        assert!(Path::new("test.db").is_file());

        fs::remove_file("test.db").await.unwrap();
    }

    #[tokio::test]
    async fn create_and_delete_rows() {
        if Path::new("test2.db").is_file() {
            fs::remove_file("test2.db").await.unwrap();
        }

        let p = DetikData::new("test2").await.unwrap();

        assert_eq!(p.visited.count().await.unwrap(), 0);
        assert!(!p.visited.is_exist("visited").await.unwrap());
        insert!(p.visited, "visited");
        assert_eq!(p.visited.count().await.unwrap(), 1);
        assert!(p.visited.is_exist("visited").await.unwrap());
        delete!(p.visited, "visited");
        assert_eq!(p.visited.count().await.unwrap(), 0);
        assert!(!p.visited.is_exist("visited").await.unwrap());

        assert_eq!(p.queued.count().await.unwrap(), 0);
        assert!(!p.queued.is_exist("queued").await.unwrap());
        insert!(p.queued, "queued");
        assert_eq!(p.queued.count().await.unwrap(), 1);
        assert!(p.queued.is_exist("queued").await.unwrap());
        delete!(p.queued, "queued");
        assert_eq!(p.queued.count().await.unwrap(), 0);
        assert!(!p.queued.is_exist("queued").await.unwrap());

        assert_eq!(p.running.count().await.unwrap(), 0);
        assert!(!p.running.is_exist("running").await.unwrap());
        insert!(p.running, "running");
        assert_eq!(p.running.count().await.unwrap(), 1);
        assert!(p.running.is_exist("running").await.unwrap());
        delete!(p.running, "running");
        assert_eq!(p.running.count().await.unwrap(), 0);
        assert!(!p.running.is_exist("running").await.unwrap());

        assert_eq!(p.warned.count().await.unwrap(), 0);
        assert!(!p.warned.is_exist("warned").await.unwrap());
        insert!(p.warned, "warned");
        assert_eq!(p.warned.count().await.unwrap(), 1);
        assert!(p.warned.is_exist("warned").await.unwrap());
        delete!(p.warned, "warned");
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
        insert!(p.results, ("results", d));
        assert_eq!(p.results.count().await.unwrap(), 1);
        assert!(p.results.is_exist("results").await.unwrap());
        delete!(p.results, "results");
        assert_eq!(p.results.count().await.unwrap(), 0);
        assert!(!p.results.is_exist("results").await.unwrap());

        fs::remove_file("test2.db").await.unwrap();
    }

    #[tokio::test]
    async fn get_queue() {
        if Path::new("test3.db").is_file() {
            fs::remove_file("test3.db").await.unwrap();
        }

        let p = DetikData::new("test3").await.unwrap();
        let queue: Vec<String> = vec![];
        assert_eq!(p.queued_get().await.unwrap(), queue);

        insert!(p.queued, "1", "2", "3");
        assert_eq!(p.queued.count().await.unwrap(), 3);
        assert_eq_fut_strings!(p.queued_get(), "1", "2", "3");

        delete!(p.queued, "2");
        assert_eq_fut_strings!(p.queued_get(), "1", "3");

        delete!(p.queued, "1");
        assert_eq_fut_strings!(p.queued_get(), "3");

        insert!(p.queued, "1");
        assert_eq_fut_strings!(p.queued_get(), "3", "1");

        delete!(p.queued, "3");
        assert_eq_fut_strings!(p.queued_get(), "1");

        delete!(p.queued, "2");
        assert_eq_fut_strings!(p.queued_get(), "1");

        delete!(p.queued, "1");
        assert_eq_fut_strings!(p.queued_get());

        fs::remove_file("test3.db").await.unwrap();
    }

    #[tokio::test]
    async fn queued_get_n() {
        if Path::new("test4.db").is_file() {
            fs::remove_file("test4.db").await.unwrap();
        }
        let p = DetikData::new("test4").await.unwrap();

        insert!(p.queued, "1", "2", "3", "4", "5");
        assert_eq_fut_strings!(p.queued_get_n(2), "1", "2");

        fs::remove_file("test4.db").await.unwrap();
    }

    #[tokio::test]
    async fn get_running() {
        if Path::new("test5.db").is_file() {
            fs::remove_file("test5.db").await.unwrap();
        }
        let p = DetikData::new("test5").await.unwrap();

        let running: Vec<String> = vec![];
        assert_eq!(p.running_get().await.unwrap(), running);

        insert!(p.running, "1", "2", "3");
        assert_eq!(p.running.count().await.unwrap(), 3);
        assert_eq_fut_strings!(p.running_get(), "1", "2", "3");

        p.running.delete("2").await.unwrap();
        assert_eq_fut_strings!(p.running_get(), "1", "3");

        p.running.delete("1").await.unwrap();
        assert_eq_fut_strings!(p.running_get(), "3");

        p.running.insert("1").await.unwrap();
        assert_eq_fut_strings!(p.running_get(), "3", "1");

        p.running.delete("3").await.unwrap();
        assert_eq_fut_strings!(p.running_get(), "1");

        p.running.delete("2").await.unwrap();
        assert_eq_fut_strings!(p.running_get(), "1");

        p.running.delete("1").await.unwrap();
        assert_eq_fut_strings!(p.running_get());

        fs::remove_file("test5.db").await.unwrap();
    }

    #[tokio::test]
    async fn merge_queue_and_running() {
        if Path::new("test6.db").is_file() {
            fs::remove_file("test6.db").await.unwrap();
        }
        let p = DetikData::new("test6").await.unwrap();

        insert!(p.queued, "1", "2", "3");
        insert!(p.running, "4", "5");
        p.merge_queue_and_running().await.unwrap();

        assert_eq_fut_strings!(p.queued_get(), "1", "2", "3", "4", "5");
        assert_eq_fut_strings!(p.running_get());

        fs::remove_file("test6.db").await.unwrap();
    }
}
