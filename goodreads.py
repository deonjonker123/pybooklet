import html
import http.cookiejar
import json
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional, Dict, List

BASE_URL = "https://www.goodreads.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)
MIN_REQUEST_INTERVAL = 2.0  # seconds between requests to Goodreads
REQUEST_TIMEOUT = 15

_lock = threading.Lock()
_last_request = 0.0
_cache: Dict[str, Optional[Dict]] = {}

_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

last_fetch: Dict = {}


class GoodreadsError(Exception):
    """Goodreads could not be reached or returned something unusable."""

def normalize_isbn(value: str) -> Optional[str]:
    if not value:
        return None

    isbn = re.sub(r"[^0-9Xx]", "", value).upper()

    if len(isbn) == 10:
        if not _valid_isbn10(isbn):
            return None
        core = "978" + isbn[:9]
        return core + _isbn13_check_digit(core)

    if len(isbn) == 13 and isbn.isdigit():
        if _isbn13_check_digit(isbn[:12]) != isbn[12]:
            return None
        return isbn

    return None


def _valid_isbn10(isbn: str) -> bool:
    if not isbn[:9].isdigit() or not (isbn[9].isdigit() or isbn[9] == "X"):
        return False
    total = sum((10 - i) * int(c) for i, c in enumerate(isbn[:9]))
    total += 10 if isbn[9] == "X" else int(isbn[9])
    return total % 11 == 0


def _isbn13_check_digit(first12: str) -> str:
    total = sum(int(c) * (1 if i % 2 == 0 else 3) for i, c in enumerate(first12))
    return str((10 - total % 10) % 10)

def _fetch(url: str) -> tuple[str, str]:

    global _last_request

    with _lock:
        wait = MIN_REQUEST_INTERVAL - (time.monotonic() - _last_request)
        if wait > 0:
            time.sleep(wait)

        request = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })

        last_fetch.clear()
        last_fetch.update({"url": url})

        try:
            with _opener.open(request, timeout=REQUEST_TIMEOUT) as response:
                body = response.read().decode("utf-8", errors="replace")
                final_url = response.geturl()
                last_fetch.update({"status": response.status, "final_url": final_url, "body": body})
        except urllib.error.HTTPError as e:
            last_fetch.update({"status": e.code, "final_url": e.geturl()})
            if e.code == 404:
                return "", url
            raise GoodreadsError(f"Goodreads returned HTTP {e.code}")
        except urllib.error.URLError as e:
            raise GoodreadsError(f"Could not reach Goodreads: {e.reason}")
        finally:
            _last_request = time.monotonic()

    return body, final_url


def _is_book_page(url: str) -> bool:
    return "/book/show/" in urllib.parse.urlparse(url).path

def lookup_isbn(isbn: str) -> Optional[Dict]:
    clean = normalize_isbn(isbn)
    if not clean:
        raise ValueError("Not a valid ISBN")

    cache_key = f"isbn:{clean}"
    if cache_key in _cache:
        return _cache[cache_key]

    body, final_url = _fetch(f"{BASE_URL}/book/isbn/{clean}")

    if not _is_book_page(final_url):
        body, final_url = _fetch(f"{BASE_URL}/search?q={clean}")

    book = parse_book_page(body, final_url) if _is_book_page(final_url) else None
    book = _fill_from_best_edition(book)
    if book and not book.get("isbn"):
        book["isbn"] = clean

    _cache[cache_key] = book
    return book


GAP_FIELDS = ("page_count", "cover_url", "synopsis", "series", "series_number")


def _fill_from_best_edition(book: Optional[Dict]) -> Optional[Dict]:
    if not book:
        return book

    best_url = book.pop("_best_edition_url", None)
    if not best_url or not any(book.get(f) in (None, "") for f in GAP_FIELDS):
        return book

    try:
        body, final_url = _fetch(best_url)
    except GoodreadsError:
        return book

    best = parse_book_page(body, final_url) if body else None
    if best:
        best.pop("_best_edition_url", None)
        for field in GAP_FIELDS:
            if book.get(field) in (None, "") and best.get(field) not in (None, ""):
                book[field] = best[field]

    return book


def lookup_url(url: str) -> Optional[Dict]:
    if url.startswith("/"):
        url = BASE_URL + url
    if not url.startswith(BASE_URL) or not _is_book_page(url):
        raise ValueError("Not a Goodreads book URL")

    url = url.split("?")[0]
    cache_key = f"url:{url}"
    if cache_key in _cache:
        return _cache[cache_key]

    body, final_url = _fetch(url)
    book = _fill_from_best_edition(parse_book_page(body, final_url) if body else None)

    _cache[cache_key] = book
    return book


def search(query: str, limit: int = 10) -> List[Dict]:
    query = (query or "").strip()
    if not query:
        return []

    body, final_url = _fetch(f"{BASE_URL}/search?q={urllib.parse.quote_plus(query)}")

    if _is_book_page(final_url):
        book = _fill_from_best_edition(parse_book_page(body, final_url))
        if not book:
            return []
        return [{
            "title": book["title"],
            "author": book["author"],
            "series": book["series"],
            "series_number": book["series_number"],
            "cover_url": book["cover_url"],
            "published_year": book["published_year"],
            "goodreads_url": book["goodreads_url"],
        }]

    results = _parse_search_results(body, limit)

    if not results:
        results = _parse_search_results_html(body, limit)

    return results


def _search_result(book: Dict) -> Dict:
    author = ((book.get("primaryContributorEdge") or {}).get("node") or {}).get("name")

    series = None
    series_number = None
    for entry in book.get("bookSeries") or []:
        series = (entry.get("series") or {}).get("title")
        series_number = _to_number(entry.get("userPosition"))
        if series:
            break

    return {
        "title": _clean_text(book.get("title")),
        "author": _clean_text(author),
        "series": _clean_text(series),
        "series_number": series_number,
        "cover_url": _full_size_cover(book.get("imageUrl")) or None,
        "published_year": _year_from_ms((book.get("details") or {}).get("publicationTime")),
        "goodreads_url": (book.get("webUrl") or f"{BASE_URL}/book/show/{book.get('legacyId')}").split("?")[0],
    }


def _parse_search_results(body: str, limit: int) -> List[Dict]:
    chunks = []
    for raw in re.findall(r'self\.__next_f\.push\(\[1,\s*"((?:[^"\\]|\\.)*)"\]\)', body, re.S):
        try:
            chunks.append(json.loads(f'"{raw}"'))
        except ValueError:
            continue
    data = "".join(chunks)

    decoder = json.JSONDecoder()
    results = []
    seen = set()

    for match in re.finditer(r'"book":\s*\{"__typename":"Book"', data):
        try:
            book, _ = decoder.raw_decode(data, match.start() + match.group(0).index("{"))
        except ValueError:
            continue

        key = book.get("legacyId") or book.get("id")
        if not book.get("title") or key in seen:
            continue
        seen.add(key)

        results.append(_search_result(book))
        if len(results) >= limit:
            break

    return results


def _parse_search_results_html(body: str, limit: int) -> List[Dict]:
    results = []
    for row in re.findall(r'<tr[^>]*itemtype="http://schema.org/Book"[^>]*>(.*?)</tr>', body, re.S):
        link = re.search(r'<a[^>]*class="bookTitle"[^>]*href="([^"]+)"', row)
        title = re.search(r'class="bookTitle".*?<span[^>]*itemprop=["\']name["\'][^>]*>(.*?)</span>', row, re.S)
        author = re.search(r'class="authorName".*?<span[^>]*itemprop=["\']name["\'][^>]*>(.*?)</span>', row, re.S)
        cover = re.search(r'<img[^>]*class="bookCover"[^>]*src="([^"]+)"', row)

        if not link or not title:
            continue

        results.append({
            "title": _clean_text(title.group(1)),
            "author": _clean_text(author.group(1)) if author else None,
            "series": None,
            "series_number": None,
            "cover_url": _full_size_cover(cover.group(1)) if cover else None,
            "published_year": None,
            "goodreads_url": BASE_URL + link.group(1).split("?")[0],
        })

        if len(results) >= limit:
            break

    return results

def parse_book_page(body: str, url: str) -> Optional[Dict]:
    book = None

    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', body, re.S)
    if match:
        try:
            book = _parse_next_data(json.loads(match.group(1)))
        except (ValueError, AttributeError, TypeError):
            book = None

    fallback = _parse_json_ld(body)

    if not book or not book.get("title"):
        book = fallback
    elif fallback:
        for key, value in fallback.items():
            if book.get(key) in (None, "", []) and value not in (None, "", []):
                book[key] = value

    if not book or not book.get("title"):
        return None

    book["goodreads_url"] = book.get("goodreads_url") or url.split("?")[0]
    return book


def _parse_next_data(next_data: Dict) -> Optional[Dict]:
    state = next_data.get("props", {}).get("pageProps", {}).get("apolloState") or {}
    if not state:
        return None

    def deref(value):
        if isinstance(value, dict) and "__ref" in value:
            return state.get(value["__ref"]) or {}
        return value or {}

    book = None
    for key, value in state.get("ROOT_QUERY", {}).items():
        if key.startswith("getBookByLegacyId"):
            book = deref(value)
            break

    if not book:
        candidates = [v for k, v in state.items() if k.startswith("Book:") and v.get("title")]
        candidates.sort(key=lambda b: 0 if b.get("bookGenres") else 1)
        book = candidates[0] if candidates else None

    if not book:
        return None

    author = deref((book.get("primaryContributorEdge") or {}).get("node")).get("name")

    series = None
    series_number = None
    for entry in book.get("bookSeries") or []:
        series = deref(entry.get("series")).get("title")
        series_number = _to_number(entry.get("userPosition"))
        if series:
            break

    genres = []
    for entry in book.get("bookGenres") or []:
        name = deref(entry.get("genre")).get("name")
        if name:
            genres.append(name)

    description = None
    for key in sorted(book.keys(), key=lambda k: 0 if k == "description" else 1):
        if key.startswith("description") and isinstance(book[key], str):
            description = book[key]
            break

    details = book.get("details") or {}
    work = deref(book.get("work"))

    best = deref(work.get("bestBook"))
    best_url = best.get("webUrl") or (f"{BASE_URL}/book/show/{best['legacyId']}" if best.get("legacyId") else None)
    if best_url and book.get("legacyId") and str(book.get("legacyId")) in best_url:
        best_url = None
    stats = work.get("stats") or {}
    published = (work.get("details") or {}).get("publicationTime") or details.get("publicationTime")

    return {
        "title": _clean_text(book.get("title")),
        "author": _clean_text(author),
        "series": _clean_text(series),
        "series_number": series_number,
        "page_count": details.get("numPages"),
        "synopsis": _html_to_text(description),
        "genre": genres[0] if genres else None,
        "genres": genres,
        "cover_url": _full_size_cover(book.get("imageUrl")),
        "isbn": normalize_isbn(details.get("isbn13") or details.get("isbn") or ""),
        "goodreads_url": book.get("webUrl"),
        "rating": stats.get("averageRating"),
        "ratings_count": stats.get("ratingsCount"),
        "published_year": _year_from_ms(published),
        "_best_edition_url": best_url,
    }


def _parse_json_ld(body: str) -> Optional[Dict]:
    for block in re.findall(r'<script type="application/ld\+json">(.*?)</script>', body, re.S):
        try:
            data = json.loads(block)
        except ValueError:
            continue

        if isinstance(data, list):
            data = next((d for d in data if isinstance(d, dict) and d.get("@type") == "Book"), None)
        if not isinstance(data, dict) or data.get("@type") != "Book":
            continue

        authors = data.get("author") or []
        if isinstance(authors, dict):
            authors = [authors]

        title, series, series_number = _split_series(_clean_text(data.get("name")))
        rating = data.get("aggregateRating") or {}

        return {
            "title": title,
            "author": _clean_text(authors[0].get("name")) if authors else None,
            "series": series,
            "series_number": series_number,
            "page_count": data.get("numberOfPages"),
            "synopsis": None,
            "genre": None,
            "genres": [],
            "cover_url": _full_size_cover(data.get("image")),
            "isbn": normalize_isbn(data.get("isbn") or ""),
            "goodreads_url": None,
            "rating": rating.get("ratingValue"),
            "ratings_count": rating.get("ratingCount"),
            "published_year": None,
        }

    return None

def _clean_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = html.unescape(re.sub(r"<[^>]+>", "", value))
    return re.sub(r"\s+", " ", text).strip() or None


def _html_to_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = re.sub(r"<br\s*/?>", "\n", value, flags=re.I)
    text = re.sub(r"</p\s*>", "\n\n", text, flags=re.I)
    text = html.unescape(re.sub(r"<[^>]+>", "", text))
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() or None


def _split_series(title: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[float]]:
    if not title:
        return None, None, None
    match = re.match(r"^(.*?)\s*\(([^()]+?),?\s*#([\d.]+)\)$", title)
    if not match:
        return title, None, None
    return match.group(1).strip(), match.group(2).strip(), _to_number(match.group(3))


def _full_size_cover(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    return re.sub(r"\._[A-Z0-9_,]+_(?=\.(jpg|jpeg|png|webp)$)", "", url, flags=re.I)


def _to_number(value) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def _year_from_ms(value) -> Optional[int]:
    try:
        return time.gmtime(int(value) / 1000).tm_year
    except (TypeError, ValueError, OverflowError, OSError):
        return None

if __name__ == "__main__":
    import sys

    args = [a for a in sys.argv[1:] if a != "--debug"]
    debug = "--debug" in sys.argv

    if not args:
        print('Usage: python goodreads.py <isbn | goodreads url | "search words"> [--debug]')
        sys.exit(1)

    arg = " ".join(args)

    try:
        if normalize_isbn(arg):
            result = lookup_isbn(arg)
        elif arg.startswith("http") or arg.startswith("/book/show/"):
            result = lookup_url(arg)
        else:
            result = search(arg)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    except GoodreadsError as e:
        print(f"Error: {e}")

    if debug:
        body = last_fetch.get("body") or ""
        print("\n--- debug ---")
        print("requested: ", last_fetch.get("url"))
        print("final url: ", last_fetch.get("final_url"))
        print("status:    ", last_fetch.get("status"))
        print("size:      ", len(body), "chars")
        print("next data: ", '<script id="__NEXT_DATA__"' in body)
        print("json-ld:   ", 'application/ld+json' in body)
        title = re.search(r"<title>(.*?)</title>", body, re.S)
        print("page title:", _clean_text(title.group(1)) if title else None)
        print("search rows:", len(re.findall(r'itemtype="http://schema.org/Book"', body)),
              "| bookTitle links:", body.count('class="bookTitle"'))

        match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', body, re.S)
        if match:
            state = json.loads(match.group(1)).get("props", {}).get("pageProps", {}).get("apolloState") or {}
            ref = next((v.get("__ref") for k, v in state.get("ROOT_QUERY", {}).items()
                        if k.startswith("getBookByLegacyId") and isinstance(v, dict)), None)
            main = state.get(ref) or {}
            print("book ref:  ", ref)
            print("book keys: ", sorted(main.keys()))
            print("details:   ", json.dumps(main.get("details"))[:600])
            print("image keys:", {k: v for k, v in main.items() if "image" in k.lower()})

        with open("goodreads_debug.html", "w", encoding="utf-8") as f:
            f.write(body)
        print("saved:      goodreads_debug.html")
