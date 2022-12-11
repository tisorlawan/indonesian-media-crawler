use indonesian_media_crawler::crawler::detik::DetikScraper;
use indonesian_media_crawler::error::CrawlerError;
use indonesian_media_crawler::persistent::Persistent;
use indonesian_media_crawler::scraper::{Scraper, ScrapingResult};
use scraper::Html;
use std::sync::Arc;
use std::time::Instant;
use tokio::{
    sync::{
        mpsc::{self, Sender},
        Mutex,
    },
    time::Duration,
};
use tracing::{debug, info, warn};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

lazy_static::lazy_static! {
    static ref LAST_REQUEST_MUTEX: Mutex<Option<Instant>> = Mutex::new(None);
    static ref REQUEST_DELAY: Duration = Duration::from_millis(200);
    static ref EXTRACTED: Mutex<u64> = Mutex::new(0);
}

async fn run_scrapper(p: Persistent, initial_queue: Vec<String>) -> Result<(), CrawlerError> {
    let current_queue = p.get_queue().await?;

    let queue = if current_queue.is_empty() {
        for q in &initial_queue {
            p.insert_queue(q).await?;
        }
        initial_queue
    } else {
        current_queue
    };

    let (tx, mut rx) = mpsc::channel(1);

    info!("Initial Queue Length: {}", queue.len());
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        for q in queue {
            tx_clone.send(Arc::new(q)).await.unwrap();
        }
    });

    let detik_scrapper = Arc::new(DetikScraper {});

    let p = Arc::new(p);
    while let Some(url) = rx.recv().await {
        if p.is_visited(url.as_ref()).await? {
            p.delete_queue(url.as_ref()).await?;
        } else {
            tokio::spawn(handle(tx.clone(), url, detik_scrapper.clone(), p.clone()));
        }
    }

    Ok(())
}

async fn handle(
    tx: Sender<Arc<String>>,
    url: Arc<String>,
    detik_scrapper: Arc<DetikScraper>,
    persistent: Arc<Persistent>,
) -> Result<(), CrawlerError> {
    persistent.insert_visited(url.as_ref()).await?;

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
    let html = reqwest::get::<&str>(url.as_str())
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    last_request_mutex.replace(now);

    let result = {
        let doc = Html::parse_document(&html);
        detik_scrapper.scrap(&doc)
    };

    match result {
        ScrapingResult::Links(links) => {
            for link in &links {
                persistent.insert_queue(link).await?;
            }
            for link in links {
                tx.send(Arc::new(link)).await.unwrap();
            }
        }
        ScrapingResult::DocumentAndLinks(doc, links) => {
            if doc.paragraphs.is_empty() {
                warn!("\nEmpty document extracted: {}\n", url);
                persistent.delete_visited(url.as_ref()).await?;
                persistent.insert_warned(url.as_ref()).await?;
            } else {
                for link in &links {
                    persistent.insert_queue(link).await?;
                }

                let mut num = EXTRACTED.lock().await;
                info!("[{}] Insert Result", *num + 1);
                *num += 1;
                persistent.insert_result(url.as_ref(), doc).await?;

                for link in links {
                    tx.send(Arc::new(link)).await.unwrap();
                }
            }
        }
    };

    persistent.delete_queue(url.as_ref()).await?;
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
