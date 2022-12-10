use scraper::Html;

pub trait Scraper {
    type Document: std::fmt::Debug;

    fn can_be_scrapped(&self, doc: &Html) -> bool;
    fn scrap(&self, doc: &Html) -> (Option<Self::Document>, Vec<String>);
}
