use crate::scraper::Scraper;
use chrono::{DateTime, FixedOffset};
use lazy_regex::regex;
use lazy_static::lazy_static;
use scraper::{Html, Selector};
use std::borrow::Borrow;
use std::fmt;
use std::string::String;

#[derive(Debug)]
pub struct DetikArticle {
    pub title: Option<String>,
    pub published_date: Option<DateTime<FixedOffset>>,
    pub description: Option<String>,
    pub thumbnail_url: Option<String>,
    pub author: Option<String>,
    pub keywords: Vec<String>,
    pub paragraphs: Vec<String>,
}

impl fmt::Display for DetikArticle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Title           : {}",
            self.title.as_ref().unwrap_or(&"None".to_string())
        )?;
        if let Some(d) = self.author.as_ref() {
            writeln!(f, "Author          : {}", d)?;
        } else {
            writeln!(f, "Author          : None")?;
        };

        if let Some(d) = self.published_date.as_ref() {
            writeln!(f, "Published Date  : {}", d)?;
        } else {
            writeln!(f, "Published Date  : None")?;
        };

        if let Some(d) = self.description.as_ref() {
            writeln!(f, "Description     : {}", d)?;
        } else {
            writeln!(f, "Description     : None")?;
        };

        if let Some(d) = self.thumbnail_url.as_ref() {
            writeln!(f, "Thumbnail       : {}", d)?;
        } else {
            writeln!(f, "Thumbnail       : None")?;
        };
        writeln!(f, "Keywords        : {}", self.keywords.join(", "))?;
        writeln!(f, "Paragraphs      : ")?;
        for p in &self.paragraphs {
            writeln!(f, "> {}", p.replace('\n', "\n  "))?;
        }

        Ok(())
    }
}

const E: &'static str = "Invalid selector";
lazy_static! {
    static ref CONTENT_TYPE_SELECTOR: Selector =
        Selector::parse(r#"meta[name="dtk:contenttype]"#).expect(E);
    static ref TITLE_SELECTOR: Selector = Selector::parse(r#"meta[property="og:title"]"#).expect(E);
    static ref DESCRIPTION_SELECTOR: Selector =
        Selector::parse(r#"meta[property="og:description"]"#).expect(E);
    static ref PUBLISH_DATE_SELECTOR: Selector =
        Selector::parse(r#"meta[name="dtk:publishdate"]"#).expect(E);
    static ref THUMBNAIL_SELECTOR: Selector =
        Selector::parse(r#"meta[name="thumbnailUrl"]"#).expect(E);
    static ref AUTHOR_SELECTOR: Selector = Selector::parse(r#"meta[name="dtk:author"]"#).expect(E);
    static ref KEYWORDS_SELECTOR: Selector =
        Selector::parse(r#"meta[name="dtk:keywords"]"#).expect(E);
    static ref TEXT_SELECTOR: Selector =
        Selector::parse(r#"div[class="detail__body-text itp_bodycontent"]"#).expect(E);
    static ref P_SELECTOR: Selector = Selector::parse("p").expect(E);
}

#[derive(Debug)]
pub struct DetikScraper {}
impl Scraper for DetikScraper {
    type Document = DetikArticle;

    fn can_be_scrapped(&self, doc: &Html) -> bool {
        match doc.select(&CONTENT_TYPE_SELECTOR).next() {
            Some(content_type) => {
                matches!(content_type.value().attr("content"), Some("singlepagenews"))
            }
            None => false,
        }
    }

    fn scrap(&self, doc: &Html) -> (Option<Self::Document>, Vec<String>) {
        let title = doc
            .select(&TITLE_SELECTOR)
            .next()
            .and_then(|el| el.value().attr("content"))
            .map(ToString::to_string);

        let description = doc
            .select(&DESCRIPTION_SELECTOR)
            .next()
            .and_then(|el| el.value().attr("content"))
            .map(ToString::to_string);

        let published_date = doc
            .select(&PUBLISH_DATE_SELECTOR)
            .next()
            .and_then(|el| el.value().attr("content"))
            .and_then(|published_date| {
                DateTime::parse_from_str(
                    &format!("{} +0700", published_date),
                    "%Y/%m/%d %H:%M:%S %z",
                )
                .ok()
            });

        let thumbnail_url = doc
            .select(&THUMBNAIL_SELECTOR)
            .next()
            .and_then(|el| el.value().attr("content"))
            .map(ToString::to_string);

        let author = doc
            .select(&AUTHOR_SELECTOR)
            .next()
            .and_then(|el| el.value().attr("content"))
            .map(ToString::to_string);

        let keywords = doc
            .select(&KEYWORDS_SELECTOR)
            .next()
            .and_then(|el| {
                el.value().attr("content").map(|s| {
                    s.split(',')
                        .map(|s| s.trim().to_string())
                        .collect::<Vec<_>>()
                })
            })
            .unwrap_or_default();

        let mut paragraphs = vec![];
        for el in doc.select(&TEXT_SELECTOR) {
            let raw_paragraphs = el.select(&P_SELECTOR);
            for p in raw_paragraphs {
                if p.value().attr("style").is_none() {
                    let p = p.inner_html().trim().replace('\n', " ");

                    if p.starts_with("<strong>Lihat juga")
                        || (p.starts_with("<a") && p.ends_with("</a>") && p.contains("embed"))
                    {
                        continue;
                    }

                    let p = regex!(r"\s+").replace_all(p.borrow(), " ");
                    let p = regex!(r"(<em>|</em>)").replace_all(p.borrow(), "");
                    let p = regex!(r"<br>").replace_all(p.borrow(), "\n");
                    let p = regex!(r"<a.*?>(?P<text>.*?)</a>").replace_all(p.borrow(), "${text}");
                    paragraphs.push(p.into_owned().trim_start_matches('\n').to_string());
                }
            }
        }
        paragraphs.dedup();
        if Some("") == paragraphs.last().map(String::as_str) {
            paragraphs.pop();
        }

        let detik_article = DetikArticle {
            title,
            published_date,
            description,
            thumbnail_url,
            author,
            keywords,
            paragraphs,
        };
        (Some(detik_article), vec![])
    }
}
