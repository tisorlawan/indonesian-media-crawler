use indonesian_media_crawler::persistent::{Persistent, Table};

async fn prune_queued() {
    let p = Persistent::new("detik").await.unwrap();
    let mut i = 0;
    for q in p.get_queue().await.unwrap() {
        if p.visited.is_exist(&q).await.unwrap() {
            p.queued.delete(&q).await.unwrap();
            i += 1;
            println!("Delete {}", i);
        } else if p.results.is_exist(&q).await.unwrap() {
            p.queued.delete(&q).await.unwrap();
            i += 1;
            println!("Delete {}", i);
        }
    }
}

#[tokio::main]
async fn main() {
    prune_queued().await;
}
