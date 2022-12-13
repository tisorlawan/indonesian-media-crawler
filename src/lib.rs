use scraper::Html;
use std::sync::Arc;
use tokio::{
    sync::mpsc,
    time::{Duration, Instant},
};
use tracing::{debug, info, warn};

pub mod detik;

mod data;
mod error;
mod utils;

pub use data::Table;
pub use error::CrawlerError;

pub trait Article {
    fn get_paragraphs(&self) -> &[String];
}

pub enum CrawlerResult<A: Article> {
    Links(Vec<String>),
    DocumentAndLinks(A, Vec<String>),
}

pub trait Crawler {
    type Document: Article + Send;

    fn can_be_scrapped(&self, doc: &Html) -> bool;
    fn crawl(&self, doc: &Html) -> CrawlerResult<Self::Document>;
    fn extract_links(&self, doc: &Html) -> Vec<String>;
}

lazy_static::lazy_static! {
    static ref LAST_REQUEST_MUTEX: tokio::sync::Mutex<Option<Instant>> = tokio::sync::Mutex::new(None);
    static ref REQUEST_DELAY: Duration = Duration::from_millis(50);
    static ref EXTRACTED_MUTEX: std::sync::Mutex<u64> = std::sync::Mutex::new(0);
}

const MAX_IN_PROGRESS: u32 = 20;

#[async_trait::async_trait]
pub trait Storage {
    type Record: Article;

    async fn queued_get(&self) -> Result<Vec<String>, CrawlerError>;
    async fn queued_get_n(&self, n: u32) -> Result<Vec<String>, CrawlerError>;
    async fn queued_insert<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError>;
    async fn queued_delete<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError>;
    async fn queued_is_exists<I: AsRef<str> + Send>(&self, item: I) -> Result<bool, CrawlerError>;

    async fn running_get(&self) -> Result<Vec<String>, CrawlerError>;
    async fn running_insert<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError>;
    async fn running_delete<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError>;
    async fn running_count(&self) -> Result<u32, CrawlerError>;
    async fn running_is_exists<I: AsRef<str> + Send>(&self, item: I) -> Result<bool, CrawlerError>;

    async fn visited_delete<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError>;
    async fn visited_is_exists<I: AsRef<str> + Send>(&self, item: I) -> Result<bool, CrawlerError>;
    async fn visited_insert<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError>;

    async fn results_count(&self) -> Result<u32, CrawlerError>;
    async fn results_insert<I: AsRef<str> + Send>(
        &self,
        (url, record): (I, Self::Record),
    ) -> Result<(), CrawlerError>;

    async fn warned_insert<I: AsRef<str> + Send>(&self, item: I) -> Result<(), CrawlerError>;

    async fn merge_queue_and_running(&self) -> Result<(), CrawlerError> {
        let running = self.running_get().await?;
        for i in running {
            self.queued_insert(i.as_str()).await?;
            self.running_delete(i.as_str()).await?;
        }
        Ok(())
    }
}

pub async fn run_scrapper<'a, C, S>(
    crawler: C,
    storage: S,
    initial_queue: Vec<String>,
) -> Result<(), CrawlerError>
where
    C: Crawler + Send + Sync + 'static,
    S: Storage<Record = C::Document> + Sync + Send + 'static,
{
    let storage = Arc::new(storage);
    let crawler = Arc::new(crawler);

    debug!(
        "Total (in progress, queue) before merge to queue: ({}, {})",
        storage.running_get().await?.len(),
        storage.queued_get().await?.len()
    );

    storage.merge_queue_and_running().await?;

    debug!(
        "Total (in progress, queue) before merge to queue: ({}, {})",
        storage.running_get().await?.len(),
        storage.queued_get().await?.len()
    );

    let queue = storage.queued_get().await?;
    let queue = if queue.is_empty() {
        for q in &initial_queue {
            storage.queued_insert(q).await?;
        }
        initial_queue
    } else {
        queue
    };

    info!("Initial queue length: {}", queue.len());

    let (tx, mut rx) = mpsc::channel::<Arc<String>>(10);

    {
        let mut extracted = EXTRACTED_MUTEX.lock().unwrap();
        *extracted = u64::from(storage.results_count().await?);
    }

    let tx_clone = tx.clone();

    let storage_clone = storage.clone();
    tokio::spawn(async move {
        loop {
            let in_progress = storage_clone.running_count().await.unwrap();
            if in_progress < MAX_IN_PROGRESS {
                for url in storage_clone
                    .queued_get_n(MAX_IN_PROGRESS - in_progress)
                    .await
                    .unwrap()
                {
                    tx_clone.send(Arc::new(url)).await.unwrap();
                }
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    });

    while let Some(url) = rx.recv().await {
        if storage.running_is_exists(url.as_str()).await? {
            storage.queued_delete(url.as_str()).await?;
        } else if storage.visited_is_exists(url.as_str()).await? {
            storage.queued_delete(url.as_str()).await?;
        } else {
            let storage_clone = Arc::clone(&storage);
            let crawler_clone = Arc::clone(&crawler);
            tokio::spawn(handle(url, crawler_clone, storage_clone));
        }
    }

    Ok(())
}

async fn handle<C, S>(
    url: Arc<String>,
    crawler: Arc<C>,
    storage: Arc<S>,
) -> Result<(), CrawlerError>
where
    C: Crawler,
    S: Storage<Record = C::Document>,
{
    let url = url.as_str();

    storage.running_insert(url).await?;
    storage.queued_delete(url).await?;

    let html = {
        let mut last_request_mutex = LAST_REQUEST_MUTEX.lock().await;
        let last_request = last_request_mutex.take();
        let now = Instant::now();
        if let Some(last_request) = last_request {
            let duration = now.duration_since(last_request);
            if duration < *REQUEST_DELAY {
                tokio::time::sleep(*REQUEST_DELAY - duration).await;
            }
        }

        debug!("Visit {}", url);
        let html = reqwest::get(url).await.unwrap().text().await.unwrap();

        last_request_mutex.replace(now);
        html
    };

    let result = {
        let doc = Html::parse_document(&html);
        crawler.crawl(&doc)
    };

    match result {
        CrawlerResult::Links(links) => {
            storage.visited_insert(url).await?;

            for link in links {
                let link = link.as_str();
                if !storage.visited_is_exists(link).await?
                    && !storage.running_is_exists(link).await?
                    && !storage.queued_is_exists(link).await?
                {
                    storage.queued_insert(link).await?;
                }
            }
        }

        CrawlerResult::DocumentAndLinks(doc, links) => {
            if doc.get_paragraphs().is_empty() {
                warn!("\nEmpty document extracted: {}\n", url);
                // We dont insert to visited if there is warning
                storage.warned_insert(url).await?;
            } else {
                storage.results_insert((url, doc)).await?;
                storage.visited_insert(url).await?;

                {
                    let mut num = EXTRACTED_MUTEX.lock().unwrap();
                    info!("[{}] Insert Result {}", *num + 1, url);
                    *num += 1;
                }

                for link in links {
                    let link = link.as_str();
                    if !storage.visited_is_exists(link).await?
                        && !storage.running_is_exists(link).await?
                        && !storage.queued_is_exists(link).await?
                    {
                        storage.queued_insert(link).await?;
                    }
                }
            }
        }
    };

    storage.running_delete(url).await?;
    Ok(())
}
