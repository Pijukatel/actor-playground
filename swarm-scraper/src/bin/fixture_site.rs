//! A local site to test the swarm against: categories, products, and a URL list file.
//! Run with `cargo run --bin fixture_site` and point the scraper at http://127.0.0.1:8787/.
//! `FIXTURE_DELAY_MS=200` makes every product page take that long, to simulate a real site.

use axum::Router;
use axum::extract::Path;
use axum::response::Html;
use axum::routing::get;

const CATEGORIES: usize = 20;
const PRODUCTS: usize = 100;

async fn index() -> Html<String> {
    let links: String = (0..CATEGORIES)
        .map(|c| format!("<li><a href=\"/cat/{c}\">Category {c}</a></li>"))
        .collect();
    Html(format!(
        "<html><head><title>Fixture shop</title></head><body><h1>Fixture shop</h1><ul>{links}</ul></body></html>"
    ))
}

async fn category(Path(c): Path<usize>) -> Html<String> {
    let links: String = (0..PRODUCTS)
        .map(|p| format!("<li><a href=\"/p/{c}/{p}\">Product {c}-{p}</a></li>"))
        .collect();
    Html(format!(
        "<html><head><title>Category {c}</title></head><body><h1>Category {c}</h1><ul class=\"products\">{links}</ul></body></html>"
    ))
}

async fn product(Path((c, p)): Path<(usize, usize)>) -> Html<String> {
    if let Some(delay) = std::env::var("FIXTURE_DELAY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
    }
    let filler = "<p>Lorem ipsum dolor sit amet, consectetur adipiscing elit.</p>".repeat(40);
    Html(format!(
        "<html><head><title>Product {c}-{p}</title><meta name=\"description\" content=\"Product {p} of category {c}\"></head>\
         <body><h1>Product {c}-{p}</h1><span class=\"price\">{}.99</span><a class=\"back\" href=\"/cat/{c}\">back</a>{filler}</body></html>",
        p + 1
    ))
}

async fn url_list() -> String {
    let mut out = String::new();
    for c in 0..CATEGORIES {
        for p in 0..PRODUCTS {
            out.push_str(&format!("http://127.0.0.1:8787/p/{c}/{p}\n"));
        }
    }
    out
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(index))
        .route("/cat/{c}", get(category))
        .route("/p/{c}/{p}", get(product))
        .route("/list.txt", get(url_list));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8787")
        .await
        .expect("bind 127.0.0.1:8787");
    println!(
        "fixture site on http://127.0.0.1:8787/ ({} product pages, list at /list.txt)",
        CATEGORIES * PRODUCTS
    );
    axum::serve(listener, app).await.expect("serve");
}
