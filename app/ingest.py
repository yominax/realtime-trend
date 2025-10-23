import os, time, datetime as dt
import feedparser, psycopg2
from urllib.parse import urlparse

DBH="dpg-d3sm1fq4d50c73eftjcg-a.oregon-postgres.render.com"
DBN="trends_db_h43t"
DBU="trends_db_h43t_user"
DBP="eKvVtXIBWTY9vS52cN4sSMfOIGrDizP4"
DBPORT=5432

FEEDS=[
 "https://www.lemonde.fr/rss/une.xml",
 "https://www.lefigaro.fr/rss/figaro_actualites.xml",
 "https://www.francetvinfo.fr/titres.rss"
]

DDL="""
CREATE TABLE IF NOT EXISTS news_articles(
  id BIGSERIAL PRIMARY KEY,
  published_ts TIMESTAMPTZ NOT NULL,
  ts_ingest    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source TEXT, title TEXT, url TEXT UNIQUE, summary TEXT, kind TEXT
);
"""

INS="""
INSERT INTO news_articles(published_ts, source, title, url, summary, kind)
VALUES (%s,%s,%s,%s,%s,%s)
ON CONFLICT (url) DO NOTHING;
"""

def conn():
    c=psycopg2.connect(host=DBH, dbname=DBN, user=DBU, password=DBP, port=DBPORT)
    c.autocommit=True
    return c

def norm_ts(e):
    if e.get("published_parsed"):
        return dt.datetime(*e.published_parsed[:6], tzinfo=dt.timezone.utc)
    return dt.datetime.now(dt.timezone.utc)

def run():
    print("Connexion DB OK, ingestion en cours...")
    with conn() as c, c.cursor() as cur:
        cur.execute(DDL)
    while True:
        total=0
        with conn() as c, c.cursor() as cur:
            for f in FEEDS:
                d=feedparser.parse(f.strip())
                src=urlparse(f).netloc
                for e in d.entries:
                    cur.execute(INS,(norm_ts(e),src,e.get("title",""),e.get("link",""),e.get("summary",""),"une"))
                    total+=1
        print(f"Insertion terminée: {total} articles.")
        time.sleep(300)  # attend 5 min

if __name__=="__main__":
    run()
