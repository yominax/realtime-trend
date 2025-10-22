#  Trends-Realtime — Médias & Tendances en Direct

Surveillez **les flux RSS francophones**, **Wikipedia** et **Hacker News**, agrégés en temps réel via **Kafka + PostgreSQL + Streamlit**.

---

##  Architecture du Projet
![Architecture](image/architecturetrendsrealtime.png)


### Composants Principaux
- **Producers**
  - `wiki_producer` → flux Wikipedia
  - `news_producer` → flux RSS + GDELT
  - `hn_producer` → flux Hacker News
- **Consumers**
  - `db_writer` → écrit dans PostgreSQL
  - `spike_aggregator` → calcule les tendances
- **UI**
  - `streamlit_app` → interface web temps réel

---

##  Démarrage Rapide

```bash
git clone https://github.com/yominax/trends-realtime.git
cd trends-realtime
docker compose up -d --build
