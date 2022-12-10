#![allow(unused)]
use deadpool_sqlite::{Config, Manager, Object, Runtime};
use indonesian_news_scraper::detik::{DetikArticle, DetikScraper};
use indonesian_news_scraper::scraper::{Scraper, ScrapingResult};
use scraper::Html;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::{self, Sender};
use tracing::{debug, error, info};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

lazy_static::lazy_static! {
    static ref LAST_REQUEST_MUTEX: tokio::sync::Mutex<Option<Instant>> = tokio::sync::Mutex::new(None);
    static ref REQUEST_DELAY: tokio::time::Duration = tokio::time::Duration::from_millis(500);
}

async fn is_table_exists(conn: &Object, table_name: &'static str) -> bool {
    conn.interact(move |conn| {
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
            .unwrap();
        let mut rows = stmt.query((table_name,)).unwrap();
        rows.next().unwrap().is_some()
    })
    .await
    .unwrap()
}

async fn create_result_table(conn: &Object) {
    info!("Initialize table result");
    conn.interact(|conn| {
        let result = conn.execute(
            r#"CREATE TABLE results (
                   url TEXT PRIMARY KEY,
                   title TEXT,
                   author TEXT,
                   published_date DATETIME,
                   description TEXT,
                   thumbnail_url TEXT,
                   keywords TEXT,
                   paragraphs TEXT
            )"#,
            (),
        );
        match result {
            Ok(n) => debug!("Create result table: {}", n),
            Err(e) => error!("Error creating result table: {}", e),
        };
    })
    .await
    .unwrap();
}

async fn insert_result(conn: &Object, url: String, doc: DetikArticle) {
    info!("Insert result {}", url);
    conn.interact(move |conn| {
        let result = conn.execute("INSERT INTO results (url, title, published_date, description, thumbnail_url, author, keywords, paragraphs) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (
            url,
            doc.title,
            doc.published_date,
            doc.description,
            doc.thumbnail_url,
            doc.author,
            doc.keywords.join("|"),
            doc.paragraphs.join("\n")
        )).unwrap();
    })
    .await
    .unwrap();
}

async fn create_queue_table(conn: &Object, initial_queue: Vec<&'static str>) {
    debug!("Initialize table queue");
    conn.interact(|conn| {
        let result = conn.execute(
            r#"CREATE TABLE queue (
                   url TEXT PRIMARY KEY
            )"#,
            (),
        );
        match result {
            Ok(n) => debug!("Status: {}", n),
            Err(e) => error!("Error creating queue table: {}", e),
        };
    })
    .await
    .unwrap();

    for seed in initial_queue {
        conn.interact(move |conn| {
            conn.execute("INSERT INTO queue (url) VALUES (?)", (seed,))
                .unwrap();
        })
        .await
        .unwrap();
    }
}

async fn get_queue(conn: &Object) -> Vec<String> {
    conn.interact(|conn| {
        let mut stmt = conn.prepare("SELECT url FROM queue").unwrap();
        let mut rows = stmt.query(()).unwrap();
        let mut queue = vec![];
        while let Some(row) = rows.next().unwrap() {
            queue.push(row.get::<usize, String>(0).unwrap());
        }
        queue
    })
    .await
    .unwrap()
}

async fn delete_from_queue(conn: &Object, item: String) {
    conn.interact(move |conn| {
        let result = conn.execute("DELETE FROM queue WHERE url=?", (item,));
        match result {
            Ok(n) => {}
            Err(e) => error!("Error delete queue: {}", e),
        };
    })
    .await
    .unwrap()
}

async fn insert_to_queue(conn: &Object, item: String) {
    conn.interact(move |conn| {
        let result = conn.execute("INSERT INTO queue (url) VALUES (?)", (item,));
        match result {
            Ok(n) => {}
            Err(rusqlite::Error::SqliteFailure(e, Some(s))) => if s.contains("UNIQUE") {},
            Err(e) => error!("Error insert queue item: {}", e),
        };
    })
    .await
    .unwrap();
}

async fn create_handled_table(conn: &Object) {
    debug!("Initialize table queue");
    conn.interact(|conn| {
        let result = conn.execute(
            r#"CREATE TABLE handled (
                   url TEXT PRIMARY KEY
            )"#,
            (),
        );
        match result {
            Ok(n) => debug!("Status: {}", n),
            Err(e) => error!("Error creating handled table: {}", e),
        };
    })
    .await
    .unwrap();
}

async fn insert_to_handled(conn: &Object, item: String) {
    conn.interact(move |conn| {
        let result = conn.execute("INSERT INTO handled (url) VALUES (?)", (item,));
        match result {
            Ok(n) => {}
            Err(rusqlite::Error::SqliteFailure(e, Some(s))) => if s.contains("UNIQUE") {},
            Err(e) => error!("Error insert handled item: {}", e),
        };
    })
    .await
    .unwrap();
}

async fn is_handled(conn: &Object, item: String) -> bool {
    conn.interact(|conn| {
        let mut stmt = conn.prepare("SELECT url FROM handled WHERE url=?").unwrap();
        let mut rows = stmt.query((item,)).unwrap();
        let mut queue = vec![];
        while let Some(row) = rows.next().unwrap() {
            queue.push(row.get::<usize, String>(0).unwrap());
        }
        !queue.is_empty()
    })
    .await
    .unwrap()
}

async fn run_scrapper() {
    let cfg = Config::new("db.sqlite3");
    let pool = cfg.create_pool(Runtime::Tokio1).unwrap();
    let conn = Arc::new(pool.get().await.unwrap());

    let initial_queue = vec!["https://sport.detik.com/sport-lain/d-6448377/air-mineral-cocok-jadi-teman-begadang-nonton-bola-ini-alasannya"];

    if !is_table_exists(&conn, "queue").await {
        create_queue_table(&conn, initial_queue).await;
    }

    if !is_table_exists(&conn, "handled").await {
        create_handled_table(&conn).await;
    }

    if !is_table_exists(&conn, "results").await {
        create_result_table(&conn).await;
    }

    let queue = get_queue(&conn).await;
    let (tx, mut rx) = mpsc::channel::<String>(1);
    let tx2 = tx.clone();

    tokio::spawn(async move {
        for q in queue {
            tx2.send(q).await.unwrap();
        }
    });

    let detik_scrapper = Arc::new(DetikScraper {});

    while let Some(url) = rx.recv().await {
        let skip = is_handled(&conn, url.clone()).await;
        if skip {
            delete_from_queue(&conn, url.clone()).await;
            continue;
        }

        tokio::spawn(handle(
            tx.clone(),
            url.clone(),
            detik_scrapper.clone(),
            conn.clone(),
        ));
    }
}

async fn handle(
    tx: Sender<String>,
    url: String,
    detik_scrapper: Arc<DetikScraper>,
    conn: Arc<Object>,
) {
    let mut last_request_mutex = LAST_REQUEST_MUTEX.lock().await;
    let last_request = last_request_mutex.take();
    let now = Instant::now();
    if let Some(last_request) = last_request {
        let duration = now.duration_since(last_request);
        if duration < *REQUEST_DELAY {
            tokio::time::sleep(*REQUEST_DELAY - duration);
        }
    }

    debug!("Get Task {}", url);
    let html = reqwest::get::<&str>(url.as_str())
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    last_request_mutex.replace(now);

    let result = {
        let doc = Html::parse_document(&html);
        let result = detik_scrapper.scrap(&doc);
        result
    };

    match result {
        ScrapingResult::Links(links) => {
            for link in links.iter() {
                insert_to_queue(&conn, link.clone()).await;
            }
            for link in links {
                tx.send(link.to_owned()).await.unwrap();
            }
        }
        ScrapingResult::DocumentAndLinks(doc, links) => {
            for link in links.iter() {
                insert_to_queue(&conn, link.clone()).await;
            }
            insert_result(&conn, url.clone(), doc).await;
            for link in links {
                tx.send(link.to_owned()).await.unwrap();
            }
        }
    };
    insert_to_handled(&conn, url.clone()).await;
    delete_from_queue(&conn, url).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| {
                "debug,html5ever=error,selectors=error,hyper=warn,reqwest=info".into()
            }),
        )
        .with(ErrorLayer::default())
        .init();

    let detik_scrapper = DetikScraper {};
    run_scrapper().await;

    // let url = "https://sport.detik.com/sport-lain/d-6448377/air-mineral-cocok-jadi-teman-begadang-nonton-bola-ini-alasannya";
    // let url = "https://www.detik.com/hikmah";
    // let url = "https://finance.detik.com/berita-ekonomi-bisnis/d-6454399/dirjen-pajak-buka-bukaan-ada-pegawainya-hidup-serumah-tanpa-menikah";
    // let url = "https://www.detik.com/";
    // let html = reqwest::get(url).await?.text().await?;
    // // println!("{}", html);
    // // let html = std::fs::read_to_string("tests/htmls/1.html").expect("Invalid file path");
    // let doc = Html::parse_document(&html);
    //
    // let result = detik_scrapper.scrap(&doc);
    //
    // match result {
    //     ScrapingResult::Links(links) => {
    //         for link in links {
    //             println!("{}", link);
    //         }
    //         println!("==== LINKS ====");
    //     }
    //     ScrapingResult::DocumentAndLinks(doc, links) => {
    //         println!("{}", doc);
    //         println!("==== LINKS ====");
    //         for link in links {
    //             println!("{}", link);
    //         }
    //         println!("==== LINKS + DOC ====");
    //     }
    // };
    Ok(())
}
