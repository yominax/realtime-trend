import os, time, argparse, hashlib, datetime as dt
from urllib.parse import urlparse
import feedparser, psycopg2
from psycopg2.extras import execute_batch

# ----------------- Configuration -----------------
DBH   = os.getenv("DB_HOST")
DBN   = os.getenv("DB_NAME")
DBU   = os.getenv("DB_USER")
DBP   = os.getenv("DB_PASS")
DBPORT= int(os.getenv("DB_PORT", "5432"))

INTERVAL = int(os.getenv("POLL_SEC", "300"))  # 5 min
KIND     = os.getenv("NEWS_KIND", "une")

# Tous les flux RSS francophones principaux
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

# ----------------- Structure DB -----------------
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

# ----------------- Fonctions -----------------


def conn():
    # Retente la connexion plusieurs fois (10x)
    for _ in range(10):
        try:
            return psycopg2.connect(
                host=DBH,
                dbname=DBN,
                user=DBU,
                password=DBP,
                port=DBPORT,
                connect_timeout=10,
                sslmode="require"  # Render impose SSL
            )
        except psycopg2.OperationalError as e:
            print(f"[DB] Connexion échouée: {e}")
            time.sleep(3)
    raise RuntimeError("Impossible de se connecter à la base PostgreSQL après plusieurs tentatives.")

def norm_ts(e):
    if e.get("published_parsed"):
        return dt.datetime(*e.published_parsed[:6], tzinfo=dt.timezone.utc)
    return dt.datetime.now(dt.timezone.utc)

def do_one_pass():
    seen = set()
    rows = []
    for f in FEEDS:
        try:
            d = feedparser.parse(f.strip())
            src = urlparse(f).netloc.replace("www.", "")
            for e in d.entries[:100]:
                link = e.get("link") or ""
                title = (e.get("title", "") or "").strip()
                if not link and not title:
                    continue
                key = hashlib.md5((title + link).encode("utf-8")).hexdigest()
                if key in seen:
                    continue
                seen.add(key)
                rows.append((
                    norm_ts(e),
                    src,
                    title,
                    link,
                    (e.get("summary", "") or "")[:600],
                    KIND
                ))
        except Exception as ex:
            print(f"[RSS] {f} -> {ex}")
    if not rows:
        print("[RSS] Aucun article trouvé.")
        return 0
    with conn() as c, c.cursor() as cur:
        execute_batch(cur, INS, rows, page_size=200)
    print(f"[RSS] +{len(rows)} articles insérés")
    return len(rows)

# ----------------- Main -----------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Exécuter une seule fois")
    args = parser.parse_args()

    assert all([DBH, DBN, DBU, DBP]), "DB_* manquants"
    with conn() as c, c.cursor() as cur:
        cur.execute(DDL)

    print("[DB] Connexion OK, début de l’ingestion…")

    if args.once:
        do_one_pass()
        print("[RSS] Passe unique terminée.")
        return

    while True:
        do_one_pass()
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
