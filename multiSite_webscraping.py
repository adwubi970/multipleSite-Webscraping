import csv
import json
import re
import time
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

USE_PLAYWRIGHT_FALLBACK = True

MAX_URLS = 300

SLEEP_SECONDS = 1.5

PERCENT_PATTERNS = [
    r"\b\d+(\.\d+)?\s*%",                               # 12% , 12.5%
    r"\b\d+(\.\d+)?\s*(percent|percentage)\b",          # 12 percent
    r"\b\d+(\.\d+)?\s*(percentage points|pp)\b",        # 5 percentage points, 5 pp
]

TIME_PATTERNS = [
    r"\b\d+(\.\d+)?\s*(hours?|hrs?)\b",                 # 2 hours, 1 hr
    r"\b\d+(\.\d+)?\s*(minutes?|mins?)\b",              # 30 minutes, 15 min
]

CHANGE_KEYWORDS = [
    "increase", "increased", "increases", "improve", "improved", "improves", "improvement",
    "decrease", "decreased", "reduces", "reduced", "reduction",
    "saved", "save", "saving", "cut", "cuts", "cutting",
    "faster", "slower", "less time", "more time", "time saved",
    "documentation", "burden", "workload", "productivity", "efficient", "efficiency"
]

def clean(s):
    if s is None:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s if s else None


def get_domain(url: str) -> str:
    return urlparse(url).netloc.lower()


def meta_content(soup, *, name=None, prop=None):
    tag = soup.find("meta", attrs={"name": name}) if name else soup.find("meta", attrs={"property": prop})
    return clean(tag.get("content")) if tag and tag.get("content") else None

def first_text(soup, selectors):
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            return clean(el.get_text(" ", strip=True))
    return None

def parse_json_ld_for_article(soup):
    """
    Extract author + datePublished from JSON-LD if present.
    Works on many news/blog sites.
    """
    for s in soup.select('script[type="application/ld+json"]'):
        raw = s.string or s.get_text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            if not isinstance(obj, dict):
                continue

            t = obj.get("@type")
            if isinstance(t, list):
                t = " ".join(map(str, t))

            if t and any(x in str(t) for x in ["NewsArticle", "Article", "BlogPosting", "ReportageNewsArticle"]):
                author = obj.get("author")
                author_name = None
                if isinstance(author, dict):
                    author_name = author.get("name")
                elif isinstance(author, list) and author:
                    if isinstance(author[0], dict):
                        author_name = author[0].get("name")
                    else:
                        author_name = str(author[0])
                elif isinstance(author, str):
                    author_name = author

                date_published = obj.get("datePublished") or obj.get("dateCreated") or obj.get("dateModified")
                return clean(author_name), clean(date_published)

    return None, None

#URL Discovery
def discover_urls_gdelt(max_urls=300, timespan="1y", retries=3):
    """
    Auto-discovers URLs using GDELT DOC 2.0.
    Uses short queries (GDELT has query length limits).
    Tries multiple queries from richer -> simpler.
    """
    endpoint = "https://api.gdeltproject.org/api/v2/doc/doc"

    queries_to_try = [
        '("generative AI" OR "AI adoption") (productivity OR "time saved" OR efficiency) sourcecountry:unitedstates',
        '"AI adoption" productivity sourcecountry:unitedstates',
        '"generative AI" "time saved" sourcecountry:unitedstates',
        '"AI adoption" "documentation burden" time saved sourcecountry:unitedstates',
    ]

    all_urls = []
    seen = set()

    for q in queries_to_try:
        params = {
            "query": q,
            "mode": "artlist",
            "format": "json",
            "sort": "datedesc",
            "maxrecords": 250,   # GDELT returns up to ~250 per query
            "timespan": timespan,
        }

        last_err = None
        for attempt in range(1, retries + 1):
            try:
                r = requests.get(endpoint, params=params, timeout=30)
                ctype = r.headers.get("Content-Type", "")
                print(f"GDELT query='{q[:60]}...' attempt {attempt}/{retries} -> status={r.status_code}, content-type={ctype}")

                if r.status_code != 200 or "json" not in ctype.lower():
                    preview = (r.text[:200] if r.text else "")
                    raise RuntimeError(f"GDELT not JSON. status={r.status_code}, type={ctype}, preview={preview!r}")

                data = r.json()
                articles = data.get("articles", []) or []

                for a in articles:
                    u = a.get("url")
                    if not u:
                        continue
                    u = u.strip()
                    if u in seen:
                        continue
                    seen.add(u)
                    all_urls.append(u)
                    if len(all_urls) >= max_urls:
                        return all_urls

                break  # success for this query

            except Exception as e:
                last_err = e
                time.sleep(2)

        if last_err:
            print("Warning: discovery query failed:", last_err)

        if len(all_urls) >= max_urls:
            break

    if not all_urls:
        raise RuntimeError("Failed to discover any URLs from GDELT. Try timespan='6m' or adjust queries_to_try.")

    return all_urls[:max_urls]

#for fetching the page html
def fetch_with_requests(url: str):
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://google.com/",
        "Connection": "keep-alive",
    })
    r = session.get(url, timeout=25)
    return r.status_code, r.text


def fetch_with_playwright(url: str):
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            locale="en-US",
        )
        page = context.new_page()

        page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ("image", "font", "media")
            else route.continue_()
        )


        page.goto(url, wait_until="domcontentloaded", timeout=60000)

        try:
            page.wait_for_selector("h1", timeout=20000)
        except Exception:
            pass

        html = page.content()
        browser.close()
        return 200, html

def extract_body_generic(soup):
    containers = [
        "article",
        "main article",
        "main",
        ".article-body",
        ".article-content",
        ".entry-content",
        "#article-body",
    ]
    container = None
    for sel in containers:
        container = soup.select_one(sel)
        if container:
            break
    if not container:
        return None

    ps = container.select("p")
    texts = []
    for p in ps:
        t = clean(p.get_text(" ", strip=True))
        if not t:
            continue
        if t.lower() in {"advertisement", "subscribe", "sign up"}:
            continue
        texts.append(t)

    if not texts:
        return clean(container.get_text(" ", strip=True))

    if len(texts) > 1 and re.search(r"\bvia getty\b|\b/getty\b", texts[0].lower()):
        texts = texts[1:]

    return "\n\n".join(texts)

def parse_page_generic(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    title = first_text(soup, ["h1"]) or meta_content(soup, prop="og:title") or first_text(soup, ["title"])
    description = meta_content(soup, name="description") or meta_content(soup, prop="og:description")

    author_ld, date_ld = parse_json_ld_for_article(soup)
    author = author_ld or meta_content(soup, name="author") or meta_content(soup, prop="article:author")

    published = (
        date_ld
        or meta_content(soup, prop="article:published_time")
        or meta_content(soup, name="pubdate")
        or meta_content(soup, name="date")
    )

    if not published:
        page_text = soup.get_text("\n", strip=True)
        m = re.search(r"Published:\s*([0-9]{1,2}\s+[A-Za-z]{3}\s+[0-9]{4})", page_text)
        if m:
            published = clean(m.group(1))

    body = extract_body_generic(soup)

    return {
        "url": url,
        "site": get_domain(url),
        "title": title,
        "author": author,
        "published_date": published,
        "description": description,
        "body": body,
        "error": None,
    }

def snippet_has_change_context(snippet: str) -> bool:
    t = (snippet or "").lower()
    return any(k in t for k in CHANGE_KEYWORDS)

def extract_percent_and_time_snippets(text: str, window: int = 90, max_hits: int = 80):
    if not text:
        return []

    hits = []

    # Percent
    for pat in PERCENT_PATTERNS:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            start = max(0, m.start() - window)
            end = min(len(text), m.end() + window)
            snippet = clean(text[start:end])
            if snippet and snippet_has_change_context(snippet):
                hits.append({"metric_type": "percent", "value": m.group(0), "snippet": snippet})
            if len(hits) >= max_hits:
                return hits

    # Time
    for pat in TIME_PATTERNS:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            start = max(0, m.start() - window)
            end = min(len(text), m.end() + window)
            snippet = clean(text[start:end])
            if snippet and snippet_has_change_context(snippet):
                hits.append({"metric_type": "time", "value": m.group(0), "snippet": snippet})
            if len(hits) >= max_hits:
                return hits

    return hits


def build_numbers_table(rows, max_snippets_per_url: int = 6):
    out = []
    for r in rows:
        if r.get("error"):
            continue

        combined = f"{r.get('description') or ''}\n\n{r.get('body') or ''}"
        snippets = extract_percent_and_time_snippets(combined, window=90, max_hits=80)

        if not snippets:
            continue

        for s in snippets[:max_snippets_per_url]:
            out.append({
                "url": r.get("url"),
                "site": r.get("site"),
                "title": r.get("title"),
                "published_date": r.get("published_date"),
                "metric_type": s["metric_type"],  # percent | time
                "value": s["value"],              # e.g. "12%" or "30 minutes"
                "context_snippet": s["snippet"],
            })
    return out

def scrape_urls(urls):
    results = []

    for i, url in enumerate(urls, start=1):
        print(f"\n[{i}/{len(urls)}] Scraping: {url}")
        try:
            status, html = fetch_with_requests(url)
            print("requests status:", status)

            if status in (403, 429) and USE_PLAYWRIGHT_FALLBACK:
                print("blocked -> trying Playwright...")
                status, html = fetch_with_playwright(url)
                print("playwright status:", status)

            if status != 200 or not html:
                results.append({
                    "url": url,
                    "site": get_domain(url),
                    "title": None,
                    "author": None,
                    "published_date": None,
                    "description": None,
                    "body": None,
                    "error": f"HTTP {status}",
                })
            else:
                results.append(parse_page_generic(html, url))

        except Exception as e:
            results.append({
                "url": url,
                "site": get_domain(url),
                "title": None,
                "author": None,
                "published_date": None,
                "description": None,
                "body": None,
                "error": str(e),
            })

        time.sleep(SLEEP_SECONDS)

    return results

def save_numbers_csv(numbers, filename="numbers.csv"):
    fieldnames = ["url", "site", "title", "published_date", "metric_type", "value", "context_snippet"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in numbers:
            w.writerow({k: row.get(k) for k in fieldnames})


if __name__ == "__main__":
    # 1) Auto-discover up to 300 URLs
    urls = discover_urls_gdelt(max_urls=MAX_URLS, timespan="1y", retries=3)

    print(f"\nDiscovered {len(urls)} URLs:")
    for u in urls[:25]:
        print(" -", u)
    if len(urls) > 25:
        print(f"... (showing first 25 of {len(urls)})")

    # 2) Scrape them
    rows = scrape_urls(urls)


    numbers = build_numbers_table(rows, max_snippets_per_url=6)
    save_numbers_csv(numbers, "numbers.csv")

    print(f"\nDone. Saved ->  numbers.csv ({len(numbers)} rows)")
