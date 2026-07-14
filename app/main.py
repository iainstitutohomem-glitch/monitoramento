from __future__ import annotations

import os
from pathlib import Path

import feedparser
import httpx
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .db import connect, init_db, rows_to_dicts, utc_now
from .matching import search_query, term_matches


ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "app" / "static"
API_KEY = os.environ.get("RADAR_API_KEY", "")

app = FastAPI(title="Radar de Imprensa Online", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=STATIC), name="static")
app.mount("/audio", StaticFiles(directory=ROOT / "storage" / "audio"), name="audio")


class TermIn(BaseModel):
    term: str
    group_name: str = "Marca"
    match_type: str = "Exata"
    status: str = "Ativo"
    priority: str = "Média"


class RadioIn(BaseModel):
    name: str
    city: str = ""
    frequency: str = ""
    status: str = "Teste"
    media_type: str = "Radio"
    stream: str = ""
    note: str = ""


def require_auth(x_radar_key: str | None = Header(default=None)) -> None:
    if API_KEY and x_radar_key != API_KEY:
        raise HTTPException(status_code=401, detail="Chave inválida")


def filtered_online_mentions(limit: int = 300) -> list[dict]:
    with connect() as conn:
        terms = rows_to_dicts(conn.execute("select * from terms where status != 'Pausado'").fetchall())
        rows = rows_to_dicts(conn.execute("select * from online_mentions order by created_at desc limit ?", (limit,)).fetchall())
    terms_by_name = {term["term"]: term for term in terms}
    filtered = []
    for row in rows:
        term = terms_by_name.get(row["term"])
        if not term:
            continue
        searchable_text = " ".join([row.get("title", ""), row.get("summary", ""), row.get("source", "")])
        if term_matches(term, searchable_text):
            filtered.append(row)
    return filtered


@app.on_event("startup")
def startup() -> None:
    init_db()


@app.get("/")
def home() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/config.js")
def config_js() -> FileResponse:
    return FileResponse(STATIC / "config.js")


@app.get("/api/health")
def health() -> dict:
    return {"ok": True, "time": utc_now()}


@app.get("/api/dashboard")
def dashboard() -> dict:
    with connect() as conn:
        terms = conn.execute("select count(*) from terms where status != 'Pausado'").fetchone()[0]
        radios = conn.execute("select count(*) from radios where media_type = 'Radio' and stream != ''").fetchone()[0]
        hits = conn.execute("select count(*) from radio_hits").fetchone()[0]
        online = len(filtered_online_mentions(1000))
        workers = rows_to_dicts(conn.execute("select * from worker_status order by last_seen desc").fetchall())
    return {"terms": terms, "radios": radios, "radio_hits": hits, "online_mentions": online, "workers": workers}


@app.get("/api/terms")
def list_terms() -> list[dict]:
    with connect() as conn:
        return rows_to_dicts(conn.execute("select * from terms order by priority, term").fetchall())


@app.post("/api/terms")
def create_term(payload: TermIn, x_radar_key: str | None = Header(default=None)) -> dict:
    require_auth(x_radar_key)
    with connect() as conn:
        conn.execute(
            "insert or ignore into terms (term, group_name, match_type, status, priority, created_at) values (?, ?, ?, ?, ?, ?)",
            (payload.term, payload.group_name, payload.match_type, payload.status, payload.priority, utc_now()),
        )
    return {"ok": True}


@app.put("/api/terms/{term_id}")
def update_term(term_id: int, payload: TermIn, x_radar_key: str | None = Header(default=None)) -> dict:
    require_auth(x_radar_key)
    with connect() as conn:
        result = conn.execute(
            """
            update terms
            set term = ?, group_name = ?, match_type = ?, status = ?, priority = ?
            where id = ?
            """,
            (payload.term, payload.group_name, payload.match_type, payload.status, payload.priority, term_id),
        )
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Termo não encontrado")
    return {"ok": True}


@app.delete("/api/terms/{term_id}")
def delete_term(term_id: int, x_radar_key: str | None = Header(default=None)) -> dict:
    require_auth(x_radar_key)
    with connect() as conn:
        result = conn.execute("delete from terms where id = ?", (term_id,))
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Termo não encontrado")
    return {"ok": True}


@app.get("/api/radios")
def list_radios() -> list[dict]:
    with connect() as conn:
        return rows_to_dicts(conn.execute("select * from radios order by city, name").fetchall())


@app.post("/api/radios")
def create_radio(payload: RadioIn, x_radar_key: str | None = Header(default=None)) -> dict:
    require_auth(x_radar_key)
    with connect() as conn:
        conn.execute(
            """
            insert into radios (name, city, frequency, status, media_type, stream, note, created_at)
            values (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (payload.name, payload.city, payload.frequency, payload.status, payload.media_type, payload.stream, payload.note, utc_now()),
        )
    return {"ok": True}


@app.put("/api/radios/{radio_id}")
def update_radio(radio_id: int, payload: RadioIn, x_radar_key: str | None = Header(default=None)) -> dict:
    require_auth(x_radar_key)
    with connect() as conn:
        result = conn.execute(
            """
            update radios
            set name = ?, city = ?, frequency = ?, status = ?, media_type = ?, stream = ?, note = ?
            where id = ?
            """,
            (
                payload.name,
                payload.city,
                payload.frequency,
                payload.status,
                payload.media_type,
                payload.stream,
                payload.note,
                radio_id,
            ),
        )
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Rádio não encontrada")
    return {"ok": True}


@app.delete("/api/radios/{radio_id}")
def delete_radio(radio_id: int, x_radar_key: str | None = Header(default=None)) -> dict:
    require_auth(x_radar_key)
    with connect() as conn:
        result = conn.execute("delete from radios where id = ?", (radio_id,))
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Rádio não encontrada")
    return {"ok": True}


@app.get("/api/radio-hits")
def radio_hits(limit: int = 100) -> list[dict]:
    with connect() as conn:
        return rows_to_dicts(
            conn.execute("select * from radio_hits order by created_at desc limit ?", (min(limit, 300),)).fetchall()
        )


@app.get("/api/online-mentions")
def online_mentions(limit: int = 100) -> list[dict]:
    return filtered_online_mentions(min(limit, 300))


@app.post("/api/search-online")
async def search_online(x_radar_key: str | None = Header(default=None)) -> dict:
    require_auth(x_radar_key)
    with connect() as conn:
        terms = rows_to_dicts(conn.execute("select * from terms where status != 'Pausado' limit 20").fetchall())

    saved = 0
    errors = []
    async with httpx.AsyncClient(timeout=15, headers={"user-agent": "InstitutoHomemRadar/1.0"}) as client:
        for term in terms:
            url = "https://news.google.com/rss/search"
            params = {"q": search_query(term), "hl": "pt-BR", "gl": "BR", "ceid": "BR:pt-419"}
            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                feed = feedparser.parse(response.text)
                with connect() as conn:
                    for item in feed.entries[:8]:
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
            except Exception as exc:  # noqa: BLE001
                errors.append({"term": term["term"], "error": str(exc)})
    return {"ok": True, "saved": saved, "errors": errors}

