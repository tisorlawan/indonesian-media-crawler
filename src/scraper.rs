use scraper::Html;

pub enum ScrapingResult<T> {
    Links(Vec<String>),
    DocumentAndLinks(T, Vec<String>),
}

pub trait Scraper {
    type Document: std::fmt::Debug;

    fn can_be_scrapped(&self, doc: &Html) -> bool;
    fn scrap(&self, doc: &Html) -> ScrapingResult<Self::Document>;
    fn scrap_links(&self, doc: &Html) -> Vec<String>;
}
