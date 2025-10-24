import os, time, json, hashlib, requests, feedparser
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2.extras import execute_batch

# ----------------- CONFIG -----------------
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@" \
             f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT','5432')}/{os.getenv('DB_NAME')}?sslmode=require"

FEEDS = [
    "https://www.bfmtv.com/rss/",
    "https://www.france24.com/fr/rss",
    "https://www.euronews.com/rss?language=fr",
    "https://www.rtbf.be/info/rss",
    "https://ici.radio-canada.ca/rss",
    "https://www.francetvinfo.fr/titres.rss",
    "https://www.20minutes.fr/feeds/rss-une.xml",
    "https://www.lemonde.fr/rss/une.xml",
    "https://www.lefigaro.fr/rss/feeds/actualite-france.xml",
    "https://www.leparisien.fr/actualites-a-la-une.xml",
    "https://www.ouest-france.fr/rss.xml",
    "https://www.midilibre.fr/rss.php",
    "https://www.ladepeche.fr/rss.xml",
    "https://www.lavoixdunord.fr/rss.xml",
    "https://www.courrierinternational.com/rss/all.xml",
    "https://www.liberation.fr/arc/outboundfeeds/rss-all/",
    "https://www.nouvelobs.com/rss.xml",
    "https://www.lepoint.fr/rss.xml",
    "https://www.challenges.fr/rss.xml",
    "https://www.sudouest.fr/rss.xml",
    "https://www.latribune.fr/rss/france.xml",
    "https://www.rfi.fr/fr/rss",
    "https://www.europe1.fr/rss.xml",
    "https://www.huffingtonpost.fr/feeds/index.xml",
    "https://www.rtl.fr/flux/rss/une-6809",
    "https://rmc.bfmtv.com/rss/info/",
    "https://www.francebleu.fr/rss/a-la-une.xml"
]

UA = "TrendsRealtimeBot/2.0 (+github.com/yominax/trends-realtime)"
HDRS = {"User-Agent": UA}

DDL = """
CREATE TABLE IF NOT EXISTS news_articles(
  id BIGSERIAL PRIMARY KEY,
  published_ts TIMESTAMPTZ NOT NULL,
  ts_ingest TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source TEXT, title TEXT, url TEXT UNIQUE, summary TEXT, kind TEXT
);
CREATE INDEX IF NOT EXISTS news_articles_pub_idx ON news_articles(published_ts DESC);
"""

INS = """
INSERT INTO news_articles(published_ts, source, title, url, summary, kind)
VALUES (%s,%s,%s,%s,%s,%s)
ON CONFLICT (url) DO NOTHING;
"""

def log(msg): print(f"{datetime.now(timezone.utc).isoformat()} [ingest] {msg}", flush=True)

def conn():
    return psycopg2.connect(DB_URL, sslmode="require")

def norm_ts(e):
    if getattr(e, "published_parsed", None):
        return datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
    return datetime.now(timezone.utc)

def pull_feed(url):
    try:
        d = feedparser.parse(requests.get(url, headers=HDRS, timeout=20).content)
        src = urlparse(url).netloc.replace("www.", "")
        rows = []
        for e in d.entries[:50]:
            rows.append((
                norm_ts(e),
                src,
                (e.get("title","") or "").strip(),
                e.get("link","") or "",
                (e.get("summary","") or "")[:600],
                "rss"
            ))
        return rows
    except Exception as ex:
        log(f"fail {url}: {ex}")
        return []

def pull_wikipedia():
    try:
        r = requests.get("https://stream.wikimedia.org/v2/stream/recentchange", timeout=20)
    except Exception:
        return []

def main():
    log("DB connect + table init")
    with conn() as c, c.cursor() as cur:
        cur.execute(DDL)
    total = 0
    for f in FEEDS:
        rows = pull_feed(f)
        if not rows: continue
        with conn() as c, c.cursor() as cur:
            execute_batch(cur, INS, rows, page_size=200)
        total += len(rows)
        log(f"{f} -> {len(rows)} articles")
    log(f"TOTAL inserted: {total}")

if __name__ == "__main__":
    main()
