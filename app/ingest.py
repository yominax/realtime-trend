import os, time, hashlib, requests, feedparser
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2.extras import execute_batch

# ---------------- CONFIG ----------------
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@" \
             f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT','5432')}/{os.getenv('DB_NAME')}?sslmode=require"

UA = "TrendsRealtimeBot/2.0 (+github.com/yominax/trends-realtime)"
HDRS = {"User-Agent": UA}

# RSS sources
FEEDS_UNE = [
    "https://www.lemonde.fr/rss/une.xml",
    "https://www.lefigaro.fr/rss/feeds/actualite-france.xml",
    "https://www.bfmtv.com/rss/",
    "https://www.france24.com/fr/rss",
    "https://www.francetvinfo.fr/titres.rss",
    "https://www.leparisien.fr/actualites-a-la-une.xml",
]
FEEDS_CONTINU = [
    "https://www.20minutes.fr/feeds/rss-une.xml",
    "https://www.euronews.com/rss?language=fr",
    "https://www.rfi.fr/fr/rss",
    "https://rmc.bfmtv.com/rss/info/",
    "https://www.huffingtonpost.fr/feeds/index.xml",
    "https://ici.radio-canada.ca/rss",
]

WIKI_DOMAIN = "fr.wikipedia.org"
WIKI_URL = f"https://{WIKI_DOMAIN}/w/api.php"

DDL = """
CREATE TABLE IF NOT EXISTS news_articles(
  id BIGSERIAL PRIMARY KEY,
  published_ts TIMESTAMPTZ NOT NULL,
  ts_ingest TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source TEXT, title TEXT, url TEXT UNIQUE, summary TEXT, kind TEXT
);
CREATE INDEX IF NOT EXISTS news_articles_published_idx ON news_articles(published_ts DESC);
CREATE INDEX IF NOT EXISTS news_articles_kind_idx ON news_articles(kind, published_ts DESC);

CREATE TABLE IF NOT EXISTS wiki_rc(
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  page TEXT, user_name TEXT, comment TEXT, delta INT, url TEXT
);
CREATE INDEX IF NOT EXISTS wiki_rc_ts_idx ON wiki_rc(ts DESC);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'unique_wiki_url' AND conrelid = 'wiki_rc'::regclass
  ) THEN
    ALTER TABLE wiki_rc ADD CONSTRAINT unique_wiki_url UNIQUE (url);
  END IF;
END $$;
"""



SQL_INS_NEWS = """
INSERT INTO news_articles(published_ts, source, title, url, summary, kind)
VALUES (%s,%s,%s,%s,%s,%s)
ON CONFLICT (url) DO NOTHING;
"""
SQL_INS_WIKI = """
INSERT INTO wiki_rc(ts, page, user_name, comment, delta, url)
VALUES (%s,%s,%s,%s,%s,%s)
ON CONFLICT (url) DO NOTHING;
"""

# ---------------- DB ----------------
def conn():
    return psycopg2.connect(DB_URL, sslmode="require")

def log(msg):
    print(f"{datetime.now(timezone.utc).isoformat()} [ingest] {msg}", flush=True)

# ---------------- RSS ----------------
def fetch_rss(feed_list, kind):
    rows = []
    for u in feed_list:
        try:
            d = feedparser.parse(requests.get(u, headers=HDRS, timeout=20).content)
            src = urlparse(u).netloc.replace("www.", "")
            for e in d.entries[:50]:
                ts = datetime.now(timezone.utc)
                if e.get("published_parsed"):
                    ts = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                rows.append((
                    ts,
                    src,
                    (e.get("title","") or "").strip(),
                    e.get("link",""),
                    (e.get("summary","") or "")[:600],
                    kind
                ))
        except Exception as ex:
            log(f"RSS fail {u}: {ex}")
    return rows

# ---------------- Wikipedia ----------------
def fetch_wiki_recent():
    params = {
        "action": "query",
        "format": "json",
        "list": "recentchanges",
        "rcprop": "title|user|comment|timestamp|sizes",
        "rcnamespace": "0",
        "rctype": "edit|new",
        "rcshow": "!bot",
        "rclimit": "20",
        "rcdir": "newer"
    }
    try:
        r = requests.get(WIKI_URL, params=params, timeout=20, headers=HDRS)
        r.raise_for_status()
        data = r.json()
        rows = []
        for rc in data.get("query", {}).get("recentchanges", []):
            newlen = rc.get("newlen", 0)
            oldlen = rc.get("oldlen", 0)
            delta = int(newlen - oldlen)
            ts = datetime.fromisoformat(rc["timestamp"].replace("Z", "+00:00"))
            rows.append((
                ts,
                rc.get("title"),
                rc.get("user"),
                rc.get("comment"),
                delta,
                f"https://{WIKI_DOMAIN}/wiki/{(rc.get('title') or '').replace(' ', '_')}"
            ))
        return rows
    except Exception as ex:
        log(f"WIKI fail: {ex}")
        return []

# ---------------- MAIN ----------------
def main():
    with conn() as c, c.cursor() as cur:
        cur.execute(DDL)

    total_news = 0
    total_wiki = 0

    # UNE
    une_rows = fetch_rss(FEEDS_UNE, "une")
    with conn() as c, c.cursor() as cur:
        execute_batch(cur, SQL_INS_NEWS, une_rows, page_size=200)
    total_news += len(une_rows)

    # CONTINU
    cont_rows = fetch_rss(FEEDS_CONTINU, "continu")
    with conn() as c, c.cursor() as cur:
        execute_batch(cur, SQL_INS_NEWS, cont_rows, page_size=200)
    total_news += len(cont_rows)

    # WIKIPEDIA
    wiki_rows = fetch_wiki_recent()
    with conn() as c, c.cursor() as cur:
        execute_batch(cur, SQL_INS_WIKI, wiki_rows, page_size=200)
    total_wiki += len(wiki_rows)

    log(f"Inserted: News={total_news}, Wiki={total_wiki}")

if __name__ == "__main__":
    main()
