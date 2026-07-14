from __future__ import annotations

import asyncio
import os

import feedparser
import httpx

from .db import connect, init_db, rows_to_dicts, upsert_worker, utc_now
from .matching import search_query, term_matches


WORKER_ID = os.environ.get("RADAR_ONLINE_WORKER_ID", f"online-worker-{os.getpid()}")
INTERVAL_SECONDS = int(os.environ.get("RADAR_ONLINE_INTERVAL_SECONDS", "900"))


def active_terms() -> list[dict]:
    with connect() as conn:
        return rows_to_dicts(conn.execute("select * from terms where status != 'Pausado' limit 50").fetchall())


async def run_once() -> int:
    saved = 0
    terms = active_terms()
    async with httpx.AsyncClient(timeout=15, headers={"user-agent": "InstitutoHomemRadar/1.0"}) as client:
        for term in terms:
            upsert_worker(WORKER_ID, "online", "running", f"Buscando {term['term']}")
            response = await client.get(
                "https://news.google.com/rss/search",
                params={"q": search_query(term), "hl": "pt-BR", "gl": "BR", "ceid": "BR:pt-419"},
            )
            response.raise_for_status()
            feed = feedparser.parse(response.text)
            with connect() as conn:
                for item in feed.entries[:10]:
                    searchable_text = " ".join(
                        [item.get("title", ""), item.get("summary", ""), item.get("source", {}).get("title", "") if isinstance(item.get("source"), dict) else ""]
                    )
                    if not term_matches(term, searchable_text):
                        continue
                    conn.execute(
                        """
                        insert or ignore into online_mentions
                        (term, title, source, url, published, summary, created_at)
                        values (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            term["term"],
                            item.get("title", ""),
                            item.get("source", {}).get("title", "") if isinstance(item.get("source"), dict) else "",
                            item.get("link", ""),
                            item.get("published", ""),
                            item.get("summary", ""),
                            utc_now(),
                        ),
                    )
                    saved += 1
    return saved


async def main_loop() -> None:
    init_db()
    while True:
        try:
            saved = await run_once()
            upsert_worker(WORKER_ID, "online", "sleeping", f"{saved} resultados processados")
        except Exception as exc:  # noqa: BLE001
            upsert_worker(WORKER_ID, "online", "warning", str(exc))
        await asyncio.sleep(INTERVAL_SECONDS)


def main() -> None:
    asyncio.run(main_loop())


if __name__ == "__main__":
    main()
