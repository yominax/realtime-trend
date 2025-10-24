import os, time, datetime as dt
import feedparser, psycopg2
from urllib.parse import urlparse

DBH = os.getenv("DB_HOST")
DBN = os.getenv("DB_NAME")
DBU = os.getenv("DB_USER")
DBP = os.getenv("DB_PASS")
DBPORT = int(os.getenv("DB_PORT", "5432"))
INTERVAL = int(os.getenv("POLL_SEC", "300"))  # 5 min
KIND = os.getenv("NEWS_KIND", "une")          # une ou continu

FEEDS = os.getenv("FEEDS_CSV",
 "https://www.bfmtv.com/rss/,"
 "https://www.france24.com/fr/rss,"
 "https://www.euronews.com/rss?language=fr,"
 "https://www.francetvinfo.fr/titres.rss,"
 "https://www.20minutes.fr/feeds/rss-une.xml,"
 "https://www.lemonde.fr/rss/une.xml,"
 "https://www.lefigaro.fr/rss/feeds/actualite-france.xml,"
 "https://www.leparisien.fr/actualites-a-la-une.xml"
).split(",")

DDL = """
CREATE TABLE IF NOT EXISTS news_articles(
  id BIGSERIAL PRIMARY KEY,
  published_ts TIMESTAMPTZ NOT NULL,
  ts_ingest    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source TEXT, title TEXT, url TEXT UNIQUE, summary TEXT, kind TEXT
);
CREATE INDEX IF NOT EXISTS news_articles_published_idx ON news_articles(published_ts DESC);
CREATE INDEX IF NOT EXISTS news_articles_kind_idx ON news_articles(kind, published_ts DESC);
"""

INS = """
INSERT INTO news_articles(published_ts, source, title, url, summary, kind)
VALUES (%s,%s,%s,%s,%s,%s)
ON CONFLICT (url) DO NOTHING;
"""

import psycopg2
import os

def conn():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        dsn = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode=require"
    c = psycopg2.connect(dsn)
    c.autocommit = True
    return c

def norm_ts(e):
    if e.get("published_parsed"):
        return dt.datetime(*e.published_parsed[:6], tzinfo=dt.timezone.utc)
    return dt.datetime.now(dt.timezone.utc)

def run():
    with conn() as c, c.cursor() as cur:
        cur.execute(DDL)
    total = 0
    with conn() as c, c.cursor() as cur:
        for f in FEEDS:
            d = feedparser.parse(f.strip())
            src = urlparse(f).netloc
            for e in d.entries:
                cur.execute(INS, (norm_ts(e), src, e.get("title", ""), e.get("link", ""),
                                  e.get("summary", ""), KIND))
                total += 1
    print(f"[ingestor] polled {len(FEEDS)} feeds, inserted {total} rows")

if __name__ == "__main__":
    run()
