pub mod detik;

use scraper::Html;

pub enum CrawlerResult<T> {
    Links(Vec<String>),
    DocumentAndLinks(T, Vec<String>),
}

pub trait Crawler {
    type Document: std::fmt::Debug;

    fn can_be_scrapped(&self, doc: &Html) -> bool;
    fn crawl(&self, doc: &Html) -> CrawlerResult<Self::Document>;
    fn extract_links(&self, doc: &Html) -> Vec<String>;
}
