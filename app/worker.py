from __future__ import annotations

import asyncio
import os
import re
import shutil
import subprocess
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from .db import connect, init_db, rows_to_dicts, upsert_worker, utc_now


ROOT = Path(__file__).resolve().parents[1]
BUFFER = ROOT / "storage" / "buffer"
AUDIO = ROOT / "storage" / "audio"
WORKER_ID = os.environ.get("RADAR_WORKER_ID", f"radio-worker-{os.getpid()}")
MAX_RADIOS = int(os.environ.get("RADAR_MAX_RADIOS", os.environ.get("RADAR_CONCURRENCY", "0")))
TRANSCRIBE_CONCURRENCY = int(os.environ.get("RADAR_TRANSCRIBE_CONCURRENCY", "2"))
CHUNK_SECONDS = int(os.environ.get("RADAR_CHUNK_SECONDS", "30"))
BEFORE_SECONDS = int(os.environ.get("RADAR_BEFORE_SECONDS", "120"))
AFTER_SECONDS = int(os.environ.get("RADAR_AFTER_SECONDS", "120"))
BEFORE_CHUNKS = max(1, BEFORE_SECONDS // CHUNK_SECONDS)
AFTER_CHUNKS = max(1, AFTER_SECONDS // CHUNK_SECONDS)
HIT_COOLDOWN_SECONDS = int(os.environ.get("RADAR_HIT_COOLDOWN_SECONDS", "240"))
MODEL_NAME = os.environ.get("RADAR_WHISPER_MODEL", "tiny")
_MODEL = None


def safe_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "radio"


def ffmpeg_path() -> str:
    exe = shutil.which("ffmpeg")
    if not exe:
        raise RuntimeError("ffmpeg nao encontrado no servidor")
    return exe


def active_terms() -> list[str]:
    with connect() as conn:
        rows = conn.execute(
            "select term from terms where status != 'Pausado' and group_name != 'Negativo'"
        ).fetchall()
    return [row["term"] for row in rows if row["term"]]


def active_radios() -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            """
            select * from radios
            where media_type = 'Radio'
              and status != 'Pausada'
              and stream != ''
            order by city, name
            """
        ).fetchall()
    return rows_to_dicts(rows)


def transcribe(path: Path) -> str:
    global _MODEL
    try:
        from faster_whisper import WhisperModel
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("faster-whisper nao instalado no worker") from exc

    if _MODEL is None:
        _MODEL = WhisperModel(MODEL_NAME, device="cpu", compute_type="int8")
    segments, _ = _MODEL.transcribe(str(path), language="pt")
    return " ".join(segment.text.strip() for segment in segments).strip()


def find_hits(text: str, terms: list[str]) -> list[str]:
    normalized = text.lower()
    return [term for term in terms if term.lower() in normalized]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def record_chunk(radio: dict, path: Path) -> tuple[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    started_at = now_iso()
    command = [
        ffmpeg_path(),
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        radio["stream"],
        "-t",
        str(CHUNK_SECONDS),
        "-ac",
        "1",
        "-ar",
        "16000",
        str(path),
    ]
    subprocess.run(command, check=True, timeout=CHUNK_SECONDS + 40, capture_output=True, text=True)
    return started_at, now_iso()


def concat_clip(radio: dict, chunks: list[tuple[Path, str, str]]) -> Path:
    AUDIO.mkdir(parents=True, exist_ok=True)
    slug = safe_slug(f"{radio['name']}-{radio.get('city', '')}")
    output = AUDIO / f"{slug}-{int(time.time())}.wav"
    list_file = BUFFER / slug / f"concat-{int(time.time())}.txt"
    list_file.write_text("".join(f"file '{chunk[0].as_posix()}'\n" for chunk in chunks), encoding="utf-8")
    command = [
        ffmpeg_path(),
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        str(list_file),
        "-c",
        "copy",
        str(output),
    ]
    subprocess.run(command, check=True, timeout=60, capture_output=True, text=True)
    return output


def save_hit(radio: dict, term: str, transcript: str, audio: Path, started_at: str, ended_at: str) -> None:
    with connect() as conn:
        conn.execute(
            """
            insert into radio_hits
            (radio_id, radio_name, city, term, transcript, audio_path, started_at, ended_at, created_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                radio["id"],
                radio["name"],
                radio.get("city", ""),
                term,
                transcript,
                f"/audio/{audio.name}",
                started_at,
                ended_at,
                utc_now(),
            ),
        )


async def record_after_window(radio: dict, slug: str) -> list[tuple[Path, str, str]]:
    chunks = []
    for _ in range(AFTER_CHUNKS):
        chunk = BUFFER / slug / f"{int(time.time())}.wav"
        started_at, ended_at = await asyncio.to_thread(record_chunk, radio, chunk)
        chunks.append((chunk, started_at, ended_at))
    return chunks


async def monitor_radio(radio: dict, slot: int, transcribe_gate: asyncio.Semaphore) -> None:
    slug = safe_slug(f"{radio['name']}-{radio.get('city', '')}")
    ring: deque[tuple[Path, str, str]] = deque(maxlen=BEFORE_CHUNKS + 1)
    last_hits: dict[str, float] = {}
    while True:
        terms = active_terms()
        upsert_worker(WORKER_ID, "radio", "running", f"{radio['name']} ({slot})", {"radio_id": radio["id"]})
        chunk = BUFFER / slug / f"{int(time.time())}.wav"
        try:
            started_at, ended_at = await asyncio.to_thread(record_chunk, radio, chunk)
            ring.append((chunk, started_at, ended_at))
            async with transcribe_gate:
                transcript = await asyncio.to_thread(transcribe, chunk)
            for term in find_hits(transcript, terms):
                if time.time() - last_hits.get(term, 0) < HIT_COOLDOWN_SECONDS:
                    continue
                last_hits[term] = time.time()
                upsert_worker(WORKER_ID, "radio", "capturing", f"{radio['name']}: {term}", {"radio_id": radio["id"]})
                before_and_current = list(ring)
                after = await record_after_window(radio, slug)
                clip_chunks = before_and_current + after
                clip = await asyncio.to_thread(concat_clip, radio, clip_chunks)
                save_hit(radio, term, transcript, clip, clip_chunks[0][1], clip_chunks[-1][2])
        except Exception as exc:  # noqa: BLE001
            upsert_worker(WORKER_ID, "radio", "warning", f"{radio['name']}: {exc}", {"radio_id": radio["id"]})
            await asyncio.sleep(10)


async def supervisor() -> None:
    init_db()
    while True:
        radios = active_radios()
        if MAX_RADIOS > 0:
            radios = radios[:MAX_RADIOS]
        if not radios:
            upsert_worker(WORKER_ID, "radio", "idle", "Nenhuma radio ativa com stream")
            await asyncio.sleep(30)
            continue
        upsert_worker(WORKER_ID, "radio", "running", f"Monitorando {len(radios)} radios")
        transcribe_gate = asyncio.Semaphore(max(1, TRANSCRIBE_CONCURRENCY))
        await asyncio.gather(*(monitor_radio(radio, index + 1, transcribe_gate) for index, radio in enumerate(radios)))


def main() -> None:
    asyncio.run(supervisor())


if __name__ == "__main__":
    main()
