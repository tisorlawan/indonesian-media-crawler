use indonesian_media_crawler::crawler::detik::DetikScraper;
use indonesian_media_crawler::crawler::{Crawler, CrawlerResult};
use indonesian_media_crawler::error::CrawlerError;
use indonesian_media_crawler::persistent::{Persistent, Table};
use scraper::Html;
use std::sync::Arc;
use std::time::Instant;
use tokio::{
    sync::{mpsc, Mutex},
    time::Duration,
};
use tracing::{debug, info, warn};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

lazy_static::lazy_static! {
    static ref LAST_REQUEST_MUTEX: Mutex<Option<Instant>> = Mutex::new(None);
    static ref REQUEST_DELAY: Duration = Duration::from_millis(50);
    static ref EXTRACTED_MUTEX: Mutex<u64> = Mutex::new(0);
}

const MAX_IN_PROGRESS: u32 = 20;

async fn run_scrapper(p: Persistent, initial_queue: Vec<String>) -> Result<(), CrawlerError> {
    let p = Arc::new(p);

    debug!(
        "Total (in progress, queue) before merge to queue: ({}, {})",
        p.get_running().await?.len(),
        p.get_queue().await?.len()
    );

    p.merge_queue_and_running().await?;

    debug!(
        "Total (in progress, queue) before merge to queue: ({}, {})",
        p.get_running().await?.len(),
        p.get_queue().await?.len()
    );

    let queue = p.get_queue().await?;
    let queue = if queue.is_empty() {
        for q in &initial_queue {
            p.queued.insert(q).await?;
        }
        initial_queue
    } else {
        queue
    };

    info!("Initial queue length: {}", queue.len());

    let (tx, mut rx) = mpsc::channel::<Arc<String>>(10);
    let detik_scrapper = Arc::new(DetikScraper {});

    {
        let mut extracted = EXTRACTED_MUTEX.lock().await;
        *extracted = u64::from(p.results.count().await?);
    }

    let tx_clone = tx.clone();

    let p_clone = p.clone();
    tokio::spawn(async move {
        loop {
            let in_progress = p_clone.running.count().await.unwrap();
            if in_progress < MAX_IN_PROGRESS {
                for url in p_clone
                    .get_queue_n(MAX_IN_PROGRESS - in_progress)
                    .await
                    .unwrap()
                {
                    tx_clone.send(Arc::new(url)).await.unwrap();
                }
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    });

    let p = p.clone();
    while let Some(url) = rx.recv().await {
        if p.running.is_exist(url.as_str()).await? {
            p.queued.delete(url.as_str()).await?;
        } else if p.visited.is_exist(url.as_str()).await? {
            p.queued.delete(url.as_str()).await?;
        } else {
            tokio::spawn(handle(url, detik_scrapper.clone(), p.clone()));
        }
    }

    Ok(())
}

async fn handle(
    url: Arc<String>,
    detik_scrapper: Arc<DetikScraper>,
    persistent: Arc<Persistent>,
) -> Result<(), CrawlerError> {
    let url = url.as_str();

    persistent.running.insert(url).await?;
    persistent.queued.delete(url).await?;

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
        detik_scrapper.crawl(&doc)
    };

    match result {
        CrawlerResult::Links(links) => {
            persistent.visited.insert(url).await?;

            for link in &links {
                persistent.queued.insert(link).await?;
            }
            for link in links {
                let link = link.as_str();
                if !persistent.visited.is_exist(link).await?
                    && !persistent.running.is_exist(link).await?
                    && !persistent.queued.is_exist(link).await?
                {
                    persistent.queued.insert(link).await?;
                }
            }
        }
        CrawlerResult::DocumentAndLinks(doc, links) => {
            if doc.paragraphs.is_empty() {
                warn!("\nEmpty document extracted: {}\n", url);
                // We dont insert to visited if there is warning
                persistent.warned.insert(url).await?;
            } else {
                persistent.results.insert((url, doc)).await?;
                persistent.visited.insert(url).await?;

                let mut num = EXTRACTED_MUTEX.lock().await;
                info!("[{}] Insert Result {}", *num + 1, url);
                *num += 1;

                for link in links {
                    let link = link.as_str();
                    if !persistent.visited.is_exist(link).await?
                        && !persistent.running.is_exist(link).await?
                        && !persistent.queued.is_exist(link).await?
                    {
                        persistent.queued.insert(link).await?;
                    }
                }
            }
        }
    };

    persistent.running.delete(url).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| {
                "debug,html5ever=error,selectors=error,hyper=warn,reqwest=info,sqlx=warn".into()
            }),
        )
        .with(ErrorLayer::default())
        .init();

    let p = Persistent::new("detik").await?;

    let initial_queue = vec!["https://travel.detik.com/travel-news/d-6454465/kadispar-badung-jamin-wisman-tak-disweeping-imbas-pasal-zina-kuhp".to_string()];
    run_scrapper(p, initial_queue).await?;

    // let detik_scrapper = DetikScraper {};
    // let url = "https://sport.detik.com/aboutthegame/detik-insider/d-5746542/para-peracik-bola-mati";
    // let url = "https://sport.detik.com/sport-lain/d-6448377/air-mineral-cocok-jadi-teman-begadang-nonton-bola-ini-alasannya";
    // let url = "https://www.detik.com/hikmah";
    // let url = "https://finance.detik.com/berita-ekonomi-bisnis/d-6454399/dirjen-pajak-buka-bukaan-ada-pegawainya-hidup-serumah-tanpa-menikah";
    // let url = "https://www.detik.com/";
    // let url = "https://travel.detik.com/travel-news/d-6454465/kadispar-badung-jamin-wisman-tak-disweeping-imbas-pasal-zina-kuhp";
    // let html = reqwest::get(url).await?.text().await.unwrap();
    // println!("{}", html);
    //
    // let html = std::fs::read_to_string("tests/htmls/travel.html").expect("Invalid file path");
    // let doc = Html::parse_document(&html);
    //
    // let result = detik_scrapper.scrap(&doc);
    // match result {
    //     ScrapingResult::Links(links) => {
    //         for link in links {
    //             println!("{}", link);
    //         }
    //         println!("==== LINKS ====");
    //     }
    //     ScrapingResult::DocumentAndLinks(doc, links) => {
    //         println!("{}", doc);
    //         p.insert_result("http://result.html".to_string(), doc)
    //             .await?;
    //         // println!("==== LINKS ====");
    //         // for link in links {
    //         //     println!("{}", link);
    //         // }
    //         // println!("==== LINKS + DOC ====");
    //     }
    // };
    Ok(())
}
