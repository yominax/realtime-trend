
import os, time, argparse, hashlib, datetime as dt
from urllib.parse import urlparse

import requests, feedparser, psycopg2
from psycopg2.extras import execute_batch

# ----------------- Config -----------------
DBH   = os.getenv("DB_HOST")
DBN   = os.getenv("DB_NAME")
DBU   = os.getenv("DB_USER")
DBP   = os.getenv("DB_PASS")
DBPORT= int(os.getenv("DB_PORT", "5432"))


POLL_NEWS = int(os.getenv("POLL_NEWS_SEC", "300"))   # 5 min
POLL_WIKI = int(os.getenv("POLL_WIKI_SEC", "30"))


FEEDS = (os.getenv("FEEDS_CSV") or
"https://www.bfmtv.com/rss/,"
"https://www.france24.com/fr/rss,"
"https://www.euronews.com/rss?language=fr,"
"https://www.rtbf.be/info/rss,"
"https://ici.radio-canada.ca/rss,"
"https://www.francetvinfo.fr/titres.rss,"
"https://www.20minutes.fr/feeds/rss-une.xml,"
"https://www.lemonde.fr/rss/une.xml,"
"https://www.lefigaro.fr/rss/feeds/actualite-france.xml,"
"https://www.leparisien.fr/actualites-a-la-une.xml,"
"https://www.ouest-france.fr/rss.xml,"
"https://www.midilibre.fr/rss.php,"
"https://www.ladepeche.fr/rss.xml,"
"https://www.lavoixdunord.fr/rss.xml,"
"https://www.courrierinternational.com/rss/all.xml,"
"https://www.liberation.fr/arc/outboundfeeds/rss-all/,"
"https://www.nouvelobs.com/rss.xml,"
"https://www.lepoint.fr/rss.xml,"
"https://www.challenges.fr/rss.xml,"
"https://www.sudouest.fr/rss.xml,"
"https://www.latribune.fr/rss/france.xml,"
"https://www.rfi.fr/fr/rss,"
"https://www.europe1.fr/rss.xml,"
"https://www.huffingtonpost.fr/feeds/index.xml,"
"https://www.rtl.fr/flux/rss/une-6809,"
"https://rmc.bfmtv.com/rss/info/,"
"https://www.francebleu.fr/rss/a-la-une.xml"
).split(",")

# Wikipedia RC
WIKI_LANG = os.getenv("WIKI_LANG", "fr").lower()
WIKI_DOMAIN = {"fr":"fr.wikipedia.org","en":"en.wikipedia.org"}.get(WIKI_LANG, "fr.wikipedia.org")

UA = "TrendsRealtimeBot/1.0 (+github.com/yominax/trends-realtime)"
HDRS = {
    "User-Agent": UA,
    "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.7",
}

# ----------------- DB -----------------
DDL = """
CREATE TABLE IF NOT EXISTS news_articles(
  id BIGSERIAL PRIMARY KEY,
  published_ts TIMESTAMPTZ NOT NULL,
  ts_ingest    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source TEXT, title TEXT, url TEXT, summary TEXT, kind TEXT
);
CREATE INDEX IF NOT EXISTS news_articles_published_idx ON news_articles(published_ts DESC);
CREATE INDEX IF NOT EXISTS news_articles_kind_idx ON news_articles(kind, published_ts DESC);
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'unique_url' AND conrelid = 'news_articles'::regclass
  ) THEN
    ALTER TABLE news_articles ADD CONSTRAINT unique_url UNIQUE (url);
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS wiki_rc(
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  page TEXT, user_name TEXT, comment TEXT, delta INT, url TEXT
);
CREATE INDEX IF NOT EXISTS wiki_rc_ts_idx ON wiki_rc(ts DESC);
"""

SQL_INS_NEWS = """
INSERT INTO news_articles(published_ts, source, title, url, summary, kind)
VALUES (%s,%s,%s,%s,%s,%s)
ON CONFLICT (url) DO NOTHING;
"""

SQL_INS_WIKI = """
INSERT INTO wiki_rc(ts, page, user_name, comment, delta, url)
VALUES (%s,%s,%s,%s,%s,%s);
"""

def db_conn():
    c = psycopg2.connect(
        host=DBH, dbname=DBN, user=DBU, password=DBP, port=DBPORT, sslmode="require"
    )
    c.autocommit = True
    return c

def db_init():
    with db_conn() as c, c.cursor() as cur:
        cur.execute(DDL)

# ----------------- Helpers -----------------
def log(s): print(dt.datetime.now(dt.timezone.utc).isoformat(), s, flush=True)

def norm_ts_from_entry(e):
    p = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
    if p:
        return dt.datetime(*p[:6], tzinfo=dt.timezone.utc)
    return dt.datetime.now(dt.timezone.utc)

def fetch_bytes(url, timeout=20):
    r = requests.get(url, headers=HDRS, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.content

# ----------------- RSS pass -----------------
def do_one_pass_news(kind="une"):
    seen = set()
    rows = []
    for u in FEEDS:
        u = u.strip()
        if not u: continue
        try:
            body = fetch_bytes(u)
            d = feedparser.parse(body)
            for e in d.entries[:100]:
                link = e.get("link") or ""
                title = (e.get("title","") or "").strip()
                if not link and not title: 
                    continue
                # 
                key = hashlib.md5((title+link).encode("utf-8")).hexdigest()
                if key in seen: 
                    continue
                seen.add(key)
                rows.append((
                    norm_ts_from_entry(e),
                    urlparse(u).netloc.replace("www.",""),
                    title,
                    link,
                    (e.get("summary","") or "")[:600],
                    kind
                ))
        except Exception as ex:
            log(f"[RSS] {u} -> {ex}")
    if not rows: 
        return 0
    with db_conn() as c, c.cursor() as cur:
        execute_batch(cur, SQL_INS_NEWS, rows, page_size=200)
    return len(rows)

# ----------------- Wikipedia RC pass -----------------
def fetch_wiki(rcstart=None, rccontinue=None):
    url = f"https://{WIKI_DOMAIN}/w/api.php"
    params = {
        "action":"query","format":"json","list":"recentchanges",
        "rcprop":"title|user|comment|timestamp|sizes",
        "rcnamespace":"0","rctype":"edit|new","rcshow":"!bot",
        "rclimit":"50","rcdir":"newer","origin":"*"
    }
    if rccontinue: params["rccontinue"] = rccontinue
    elif rcstart:  params["rcstart"] = rcstart
    r = requests.get(url, params=params, timeout=20, headers={"User-Agent": UA})
    r.raise_for_status()
    return r.json()

def do_one_pass_wiki(since_iso):
    try:
        data = fetch_wiki(rcstart=since_iso)
    except Exception as ex:
        log(f"[WIKI] fetch err: {ex}")
        return 0, None
    rows = []
    for rc in data.get("query", {}).get("recentchanges", []):
        newlen = rc.get("newlen") or 0
        oldlen = rc.get("oldlen") or 0
        ts = dt.datetime.fromisoformat(rc["timestamp"].replace("Z","+00:00"))
        rows.append((
            ts,
            rc.get("title"),
            rc.get("user"),
            rc.get("comment"),
            int(newlen - oldlen),
            f"https://{WIKI_DOMAIN}/wiki/{(rc.get('title') or '').replace(' ','_')}"
        ))
    if rows:
        with db_conn() as c, c.cursor() as cur:
            execute_batch(cur, SQL_INS_WIKI, rows, page_size=200)
    rccont = data.get("continue", {}).get("rccontinue")
    return len(rows), rccont

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Exécute une passe puis quitte")
    parser.add_argument("--kind", default="une", help="Tag de flux news: une|continu")
    args = parser.parse_args()

    assert all([DBH, DBN, DBU, DBP]), "DB_* manquants"
    db_init()
    log("DB OK; start ingest")

    if args.once:
        n = do_one_pass_news(args.kind)
        w,_ = do_one_pass_wiki((dt.datetime.now(dt.timezone.utc)-dt.timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))
        log(f"once -> news:{n}, wiki:{w}")
        return

    # boucl
    next_news = 0.0
    since_iso = (dt.datetime.now(dt.timezone.utc)-dt.timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    while True:
        now = time.time()
        if now >= next_news:
            n = do_one_pass_news(args.kind)
            if n: log(f"[RSS] +{n} rows")
            next_news = now + POLL_NEWS

        w, rccont = do_one_pass_wiki(since_iso)
        if w: log(f"[WIKI] +{w} rows")
        time.sleep(POLL_WIKI)

if __name__ == "__main__":
    main()
