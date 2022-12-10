use indonesian_news_scraper::detik::DetikScraper;
use indonesian_news_scraper::scraper::{Scraper, ScrapingResult};
use scraper::Html;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let detik_scrapper = DetikScraper {};

    // let url = "https://sport.detik.com/sport-lain/d-6448377/air-mineral-cocok-jadi-teman-begadang-nonton-bola-ini-alasannya";
    // let url = "https://www.detik.com/hikmah";
    // let url = "https://finance.detik.com/berita-ekonomi-bisnis/d-6454399/dirjen-pajak-buka-bukaan-ada-pegawainya-hidup-serumah-tanpa-menikah";
    // let html = reqwest::get(url).await?.text().await?;
    // println!("{}", html);
    let html = std::fs::read_to_string("tests/htmls/1.html").expect("Invalid file path");
    let doc = Html::parse_document(&html);

    let result = detik_scrapper.scrap(&doc);

    match result {
        ScrapingResult::Links(_links) => todo!(),
        ScrapingResult::DocumentAndLinks(doc, _links) => {
            println!("{}", doc);
        }
    };
    Ok(())
}
