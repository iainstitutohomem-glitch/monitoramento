from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(os.environ.get("RADAR_DB", ROOT / "storage" / "radar.db"))
RADIOS_SEED = ROOT / "data" / "radios.json"
TERMS_SEED = ROOT / "data" / "terms.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma journal_mode=WAL")
    conn.execute("pragma busy_timeout=30000")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def init_db() -> None:
    with connect() as conn:
        conn.executescript(
            """
            create table if not exists terms (
              id integer primary key autoincrement,
              term text not null unique,
              group_name text default 'Marca',
              match_type text default 'Exata',
              status text default 'Ativo',
              priority text default 'Média',
              created_at text not null
            );

            create table if not exists radios (
              id integer primary key autoincrement,
              name text not null,
              city text default '',
              frequency text default '',
              status text default 'Teste',
              media_type text default 'Radio',
              stream text default '',
              tudo_radio_id text default '',
              match_confidence text default '',
              note text default '',
              created_at text not null
            );

            create table if not exists online_mentions (
              id integer primary key autoincrement,
              term text not null,
              title text not null,
              source text default '',
              url text default '',
              published text default '',
              summary text default '',
              created_at text not null,
              unique(term, url)
            );

            create table if not exists radio_hits (
              id integer primary key autoincrement,
              radio_id integer,
              radio_name text not null,
              city text default '',
              term text not null,
              transcript text default '',
              audio_path text default '',
              started_at text default '',
              ended_at text default '',
              created_at text not null
            );

            create table if not exists radio_checks (
              radio_id integer primary key,
              radio_name text not null,
              city text default '',
              status text not null,
              transcript text default '',
              matched_terms text default '',
              last_audio_path text default '',
              last_recorded_at text default '',
              last_transcribed_at text default '',
              started_at text default '',
              ended_at text default '',
              error text default '',
              updated_at text not null
            );

            create table if not exists radio_segments (
              id integer primary key autoincrement,
              radio_id integer not null,
              radio_name text not null,
              city text default '',
              audio_path text not null,
              status text not null default 'recorded',
              transcript text default '',
              matched_terms text default '',
              started_at text not null,
              ended_at text not null,
              error text default '',
              created_at text not null,
              updated_at text not null
            );

            create index if not exists idx_radio_segments_radio_time
              on radio_segments (radio_id, started_at);

            create index if not exists idx_radio_segments_status_time
              on radio_segments (status, created_at);

            create table if not exists worker_status (
              worker_id text primary key,
              kind text not null,
              status text not null,
              current_task text default '',
              last_seen text not null,
              meta_json text default '{}'
            );
            """
        )
        columns = {row["name"] for row in conn.execute("pragma table_info(radio_checks)").fetchall()}
        if "last_audio_path" not in columns:
            conn.execute("alter table radio_checks add column last_audio_path text default ''")
        if "last_recorded_at" not in columns:
            conn.execute("alter table radio_checks add column last_recorded_at text default ''")
        if "last_transcribed_at" not in columns:
            conn.execute("alter table radio_checks add column last_transcribed_at text default ''")
    seed_db()


def seed_db() -> None:
    with connect() as conn:
        if conn.execute("select count(*) from radios").fetchone()[0] == 0 and RADIOS_SEED.exists():
            radios = json.loads(RADIOS_SEED.read_text(encoding="utf-8"))
            for radio in radios:
                conn.execute(
                    """
                    insert into radios
                    (name, city, frequency, status, media_type, stream, tudo_radio_id, match_confidence, note, created_at)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        radio.get("name", ""),
                        radio.get("city", ""),
                        radio.get("frequency", ""),
                        radio.get("status", "Teste"),
                        radio.get("mediaType", "Radio"),
                        radio.get("stream", ""),
                        str(radio.get("tudoRadioId", "")),
                        radio.get("matchConfidence", ""),
                        radio.get("note", ""),
                        utc_now(),
                    ),
                )

        if conn.execute("select count(*) from terms").fetchone()[0] == 0 and TERMS_SEED.exists():
            terms = json.loads(TERMS_SEED.read_text(encoding="utf-8"))
            for term in terms:
                conn.execute(
                    """
                    insert or ignore into terms
                    (term, group_name, match_type, status, priority, created_at)
                    values (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        term.get("term", ""),
                        term.get("group", "Marca"),
                        term.get("match", "Exata"),
                        term.get("status", "Ativo"),
                        term.get("priority", "Média"),
                        utc_now(),
                    ),
                )


def upsert_worker(worker_id: str, kind: str, status: str, current_task: str = "", meta: dict[str, Any] | None = None) -> None:
    with connect() as conn:
        conn.execute(
            """
            insert into worker_status (worker_id, kind, status, current_task, last_seen, meta_json)
            values (?, ?, ?, ?, ?, ?)
            on conflict(worker_id) do update set
              status=excluded.status,
              current_task=excluded.current_task,
              last_seen=excluded.last_seen,
              meta_json=excluded.meta_json
            """,
            (worker_id, kind, status, current_task, utc_now(), json.dumps(meta or {}, ensure_ascii=False)),
        )
