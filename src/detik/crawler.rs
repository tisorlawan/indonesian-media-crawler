use crate::{detik::DetikArticle, Crawler, CrawlerResult};
use chrono::DateTime;
use itertools::Itertools;
use lazy_regex::regex;
use lazy_static::lazy_static;
use scraper::{Html, Selector};
use std::borrow::Borrow;

const E: &str = "Invalid selector";
lazy_static! {
    static ref CONTENT_TYPE: Selector =
        Selector::parse(r#"meta[name="dtk:contenttype"]"#).expect(E);
    static ref TITLE: Selector = Selector::parse(r#"meta[property="og:title"]"#).expect(E);
    static ref DESCRIPTION: Selector =
        Selector::parse(r#"meta[property="og:description"]"#).expect(E);
    static ref PUBLISH_DATE: Selector =
        Selector::parse(r#"meta[name="dtk:publishdate"]"#).expect(E);
    static ref THUMBNAIL: Selector = Selector::parse(r#"meta[name="thumbnailUrl"]"#).expect(E);
    static ref AUTHOR: Selector = Selector::parse(r#"meta[name="dtk:author"]"#).expect(E);
    static ref KEYWORDS: Selector = Selector::parse(r#"meta[name="dtk:keywords"]"#).expect(E);
    static ref BODY1: Selector =
        Selector::parse(r#"div[class="detail__body-text itp_bodycontent"]"#).expect(E);
    static ref BODY_SPORT: Selector = Selector::parse(r#"div[class="detail_text"]"#).expect(E);
    static ref BODY_INET: Selector =
        Selector::parse(r#"div[class="itp_bodycontent detail__body-text"]"#).expect(E);
    static ref BODY_TRAVEL: Selector = Selector::parse(r#"div[id="detikdetailtext"]"#).expect(E);
    static ref P: Selector = Selector::parse("p").expect(E);
    static ref A: Selector = Selector::parse("a").expect(E);
}

#[derive(Debug)]
pub struct DetikCrawler;

impl Crawler for DetikCrawler {
    type Document = DetikArticle;

    fn can_be_scrapped(&self, doc: &Html) -> bool {
        match doc.select(&CONTENT_TYPE).next() {
            Some(content_type) => {
                matches!(content_type.value().attr("content"), Some("singlepagenews"))
            }
            None => false,
        }
    }

    fn extract_links(&self, doc: &Html) -> Vec<String> {
        doc.select(&A)
            .into_iter()
            .filter_map(|a| a.value().attr("href"))
            .map(str::trim)
            .filter(|l| {
                !l.is_empty()
                    && !l.starts_with('#')
                    && l.contains("detik.com")
                    && l.starts_with("https://")
            })
            .filter_map(|s| match reqwest::Url::parse(s).ok() {
                Some(url) => url.host().and_then(|host| {
                    if host.to_string().contains("detik.com") {
                        Some(s)
                    } else {
                        None
                    }
                }),
                None => None,
            })
            .map(|s| s.trim_end_matches('/'))
            .sorted()
            .dedup()
            .map(ToString::to_string)
            .collect()
    }

    fn crawl(&self, doc: &Html) -> CrawlerResult<Self::Document> {
        let links = self.extract_links(doc);

        if !self.can_be_scrapped(doc) {
            return CrawlerResult::Links(links);
        }

        let title = doc
            .select(&TITLE)
            .next()
            .and_then(|el| el.value().attr("content"))
            .map(ToString::to_string);

        let description = doc
            .select(&DESCRIPTION)
            .next()
            .and_then(|el| el.value().attr("content"))
            .map(ToString::to_string);

        let published_date = doc
            .select(&PUBLISH_DATE)
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
            .select(&THUMBNAIL)
            .next()
            .and_then(|el| el.value().attr("content"))
            .map(ToString::to_string);

        let author = doc
            .select(&AUTHOR)
            .next()
            .and_then(|el| el.value().attr("content"))
            .map(ToString::to_string);

        let keywords = doc
            .select(&KEYWORDS)
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
        for el in doc
            .select(&BODY1)
            .chain(doc.select(&BODY_SPORT))
            .chain(doc.select(&BODY_INET))
            .chain(doc.select(&BODY_TRAVEL))
        {
            let raw_paragraphs = el.select(&P);
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
                    let p = regex!(r"<strong>-+</strong>").replace_all(p.borrow(), " ");
                    let p = p.into_owned().trim_start_matches('\n').trim().to_string();

                    if p.starts_with("<strong>Artikel ini telah naik") {
                        continue;
                    }
                    if !p.is_empty() {
                        paragraphs.push(p);
                    }
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
        CrawlerResult::DocumentAndLinks(detik_article, links)
    }
}
