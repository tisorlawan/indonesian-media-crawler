use indonesian_news_scrapper::detik::DetikScrapper;
use indonesian_news_scrapper::scrapper::Scrapper;
use scraper::Html;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let detik_scrapper = DetikScrapper {};

    // let url = "https://sport.detik.com/sport-lain/d-6448377/air-mineral-cocok-jadi-teman-begadang-nonton-bola-ini-alasannya";
    // let url = "https://www.detik.com/hikmah";
    let url = "https://finance.detik.com/berita-ekonomi-bisnis/d-6454399/dirjen-pajak-buka-bukaan-ada-pegawainya-hidup-serumah-tanpa-menikah";
    let html = reqwest::get(url).await?.text().await?;
    // println!("{}", html);
    // let html = std::fs::read_to_string("tests/htmls/2.html").unwrap();
    let doc = Html::parse_document(&html);

    let (article, _links) = detik_scrapper.scrap(&doc);

    if article.is_some() {
        println!("{}", article.unwrap());
    } else {
        println!("No Article");
    }
    Ok(())
}
