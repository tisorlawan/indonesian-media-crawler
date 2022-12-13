use indonesian_media_crawler::detik::{DetikCrawler, DetikData};
use indonesian_media_crawler::run_scrapper;
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

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

    let crawler = DetikCrawler;
    let storage = DetikData::new("detik").await?;

    let initial_queue = vec!["https://travel.detik.com/travel-news/d-6454465/kadispar-badung-jamin-wisman-tak-disweeping-imbas-pasal-zina-kuhp".to_string()];
    run_scrapper(crawler, storage, initial_queue).await?;

    // let url = "https://sport.detik.com/aboutthegame/detik-insider/d-5746542/para-peracik-bola-mati";
    // let url = "https://sport.detik.com/sport-lain/d-6448377/air-mineral-cocok-jadi-teman-begadang-nonton-bola-ini-alasannya";
    // let url = "https://www.detik.com/hikmah";
    // let url = "https://finance.detik.com/berita-ekonomi-bisnis/d-6454399/dirjen-pajak-buka-bukaan-ada-pegawainya-hidup-serumah-tanpa-menikah";
    // let url = "https://www.detik.com/";
    // let url = "https://travel.detik.com/travel-news/d-6454465/kadispar-badung-jamin-wisman-tak-disweeping-imbas-pasal-zina-kuhp";
    //
    // use indonesian_media_crawler::{Crawler, CrawlerResult};
    // use scraper::Html;
    // let crawler = DetikCrawler;
    // let html = reqwest::get(url).await?.text().await.unwrap();
    // println!("{}", html);
    //
    // let html = std::fs::read_to_string("tests/htmls/travel.html").expect("Invalid file path");
    // let doc = Html::parse_document(&html);
    //
    // let result = crawler.crawl(&doc);
    // match result {
    //     CrawlerResult::Links(links) => {
    //         for link in links {
    //             println!("{}", link);
    //         }
    //         println!("==== LINKS ====");
    //     }
    //     CrawlerResult::DocumentAndLinks(doc, links) => {
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
