use crate::scraper::{Scraper, ScrapingResult};
use chrono::{DateTime, FixedOffset};
use itertools::Itertools;
use lazy_regex::regex;
use lazy_static::lazy_static;
use scraper::{Html, Selector};
use std::borrow::Borrow;
use std::fmt;
use std::string::String;

#[derive(Debug, PartialEq, Eq)]
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
    static ref P: Selector = Selector::parse("p").expect(E);
    static ref A: Selector = Selector::parse("a").expect(E);
}

#[derive(Debug)]
pub struct DetikScraper {}
impl Scraper for DetikScraper {
    type Document = DetikArticle;

    fn can_be_scrapped(&self, doc: &Html) -> bool {
        match doc.select(&CONTENT_TYPE).next() {
            Some(content_type) => {
                matches!(content_type.value().attr("content"), Some("singlepagenews"))
            }
            None => false,
        }
    }

    fn scrap_links(&self, doc: &Html) -> Vec<String> {
        doc.select(&A)
            .into_iter()
            .filter_map(|a| a.value().attr("href"))
            .map(|a| a.trim())
            .filter(|l| {
                !l.is_empty()
                    && !l.starts_with("#")
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
            .map(|s| s.trim_end_matches("/"))
            .sorted()
            .dedup()
            .map(ToString::to_string)
            .collect()
    }

    fn scrap(&self, doc: &Html) -> ScrapingResult<Self::Document> {
        let links = self.scrap_links(&doc);

        if !self.can_be_scrapped(doc) {
            return ScrapingResult::Links(links);
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
                    let p = p.into_owned().trim_start_matches('\n').trim().to_string();
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
        ScrapingResult::DocumentAndLinks(detik_article, links)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scraper::Scraper;
    use pretty_assertions::assert_eq;
    use scraper::html::Html;
    use std::fs;

    #[test]
    fn test_parsing_document_and_links() {
        let s = DetikScraper {};
        let html = fs::read_to_string("tests/htmls/1.html").expect("Invalid file url");
        let html = Html::parse_document(&html);

        let res = s.scrap(&html);
        assert!(matches!(&res, ScrapingResult::DocumentAndLinks(_, _)));

        let ScrapingResult::DocumentAndLinks(extracted_doc, _) = res else {
            unreachable!()
        };

        let doc = DetikArticle {
            title: Some(
                "Polisi soal Pistol di Kasus Cekcok Pemobil vs Pemotor: Cuma Diperlihatkan"
                    .to_string(),
            ),
            published_date: Some(
                DateTime::parse_from_str("2022/12/10 13:19:56 +0700", "%Y/%m/%d %H:%M:%S %z")
                    .expect("Invalid date format"),
            ),
            description: Some("Polisi menjelaskan soal pistol yang dibawa pengemudi mobil yang cekcok dengan pemotor di Jaksel. Pistol itu tak ditodongkan, hanya diperlihatkan.".to_string()),
            thumbnail_url: Some("https://akcdn.detik.net.id/community/media/visual/2020/03/05/043c2d4e-732c-4ff2-8922-32d98c0f0a7e_169.jpeg?w=650".to_string()),
            author: Some("Mulia Budi".to_string()),
            keywords: vec![
                "pria berpistol".to_string(),
                "cekcok".to_string(),
                "cekcok di jalan".to_string(),
                "viral".to_string(),
                "polsek kebayoran lama".to_string(),
                "jabodetabek".to_string()
            ],
            paragraphs: vec![
                "Polisi masih mendalami percekcokan antara pemotor dan pemobil yang dinarasikan membawa pistol di Cipulir, Kabayoran Lama, Jakarta Selatan (Jaksel). Korban atau pemotor pria berinisial CE, telah membuat laporan terkait kejadian itu.".to_string(),
                r#""Korbanya kita dampingi buat laporan, korbannya, kemarin. Kemarin kita dampingi untuk buat laporan, terus diambil keterangannya terhadap kejadian waktu itu," kata Kapolsek Kabayoran Lama, Kompol Widya Agustiono saat dihubungi wartawan, Sabtu (10/12/2022)."#.to_string(),
                "Widya mengatakan pemobil atau pria berkemeja biru muda dalam video tersebut menyimpan benda yang dicurigai merupakan pistol di pinggang. Dia menyebut pria itu tak mengacungkan benda menyerupai pistol itu pada CE.".to_string(),
                r#""Kalau dari keterangannya (korban), dia (pria berkemeja biru) mengeluarkan, memperlihatkan, setelah itu ditaruh di pinggang, seperti itu. Kalau langsung mengacungkan, keterangannya belum ada," ujarnya."#.to_string(),
                "Widya mengatakan pihaknya belum bisa memastikan apakah benda yang dibawa pelaku itu pistol asli atau hanya replika. Dia menegaskan polisi masih mengusut kasus tersebut.".to_string(),
                r#""(Terduga) pelakunya masih penyelidikan, belum (diketahui pistol beneran atau replika), karena kita harus berhasil dulu mengidentifikasi," ujar Widya."#.to_string(),
                "Sebagai informasi, dalam video yang beredar, pria berkemeja biru muda tampak berusaha menyerang pria yang mengenakan sweater putih. Pria berkemeja biru muda itu juga terlihat memukul wajah pria sweater putih tersebut.".to_string(),
                "Sebelumnya, sebuah video yang memperlihatkan percekcokan dua orang pria di Cipulir, Kebayoran Lama, Jakarta Selatan (Jaksel), viral di media sosial. Salah satu pria berkemeja biru muda dalam video itu dinarasikan membawa pistol.".to_string(),
                "Dalam video yang beredar, pria berkemeja biru muda tampak cekcok dengan pria yang mengenakan sweater putih. Warga tampak berkerumun melihat keributan tersebut.".to_string(),
                "Pria berbaju biru muda itu tampak berusaha menyerang pria berbaju putih. Dia juga sempat menampar wajah pria baju putih tersebut.".to_string(),
                "Kemudian, seorang satpam mencoba melerai keributan tersebut. Pria berbaju biru muda itu dinarasikan membawa pistol hingga sempat menodongkan pistol tersebut.".to_string(),
                r#""Videoin...videoin..videoin, beceng..beceng...bawa beceng. Viralin...viralin, bawa beceng itu dia," kata perekam suara dalam video tersebut."#.to_string(),
                "Peristiwa itu terjadi pada Rabu (7/12/2022) sekitar pukul 21.45 WIB. Disebut-sebut percekcokan itu terjadi antara pengemudi mobil dengan pengemudi motor.".to_string()
            ],
        };
        assert_eq!(extracted_doc, doc);
    }
}
