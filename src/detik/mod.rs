mod crawler;
mod data;

pub use crawler::DetikCrawler;
pub use data::DetikData;

use crate::Article;

use chrono::{DateTime, FixedOffset};
use std::{fmt, string::String};

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

impl Article for DetikArticle {
    fn get_paragraphs(&self) -> &[String] {
        self.paragraphs.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use crate::detik::crawler::DetikCrawler;
    use crate::Crawler;
    use crate::CrawlerResult;

    use super::*;
    use pretty_assertions::assert_eq;
    use scraper::html::Html;
    use std::fs;

    #[test]
    fn test_parsing_document_and_links() {
        let s = DetikCrawler {};
        let html = fs::read_to_string("tests/htmls/1.html").expect("Invalid file url");
        let html = Html::parse_document(&html);

        let res = s.crawl(&html);
        assert!(matches!(&res, CrawlerResult::DocumentAndLinks(_, _)));

        let CrawlerResult::DocumentAndLinks(extracted_doc, _) = res else {
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
