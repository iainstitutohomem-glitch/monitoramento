from __future__ import annotations

import asyncio
import os
import re
import shutil
import subprocess
import time
from collections import deque
from pathlib import Path

from .db import connect, init_db, rows_to_dicts, upsert_worker, utc_now


ROOT = Path(__file__).resolve().parents[1]
BUFFER = ROOT / "storage" / "buffer"
AUDIO = ROOT / "storage" / "audio"
WORKER_ID = os.environ.get("RADAR_WORKER_ID", f"radio-worker-{os.getpid()}")
CONCURRENCY = int(os.environ.get("RADAR_CONCURRENCY", "4"))
CHUNK_SECONDS = int(os.environ.get("RADAR_CHUNK_SECONDS", "30"))
BEFORE_CHUNKS = max(1, 120 // CHUNK_SECONDS)
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


def record_chunk(radio: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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


def concat_clip(radio: dict, chunks: list[Path]) -> Path:
    AUDIO.mkdir(parents=True, exist_ok=True)
    slug = safe_slug(f"{radio['name']}-{radio.get('city', '')}")
    output = AUDIO / f"{slug}-{int(time.time())}.wav"
    list_file = BUFFER / slug / f"concat-{int(time.time())}.txt"
    list_file.write_text("".join(f"file '{chunk.as_posix()}'\n" for chunk in chunks), encoding="utf-8")
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


def save_hit(radio: dict, term: str, transcript: str, audio: Path) -> None:
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
                "",
                "",
                utc_now(),
            ),
        )


async def monitor_radio(radio: dict, slot: int) -> None:
    slug = safe_slug(f"{radio['name']}-{radio.get('city', '')}")
    ring: deque[Path] = deque(maxlen=BEFORE_CHUNKS + 1)
    while True:
        terms = active_terms()
        upsert_worker(WORKER_ID, "radio", "running", f"{radio['name']} ({slot})", {"radio_id": radio["id"]})
        chunk = BUFFER / slug / f"{int(time.time())}.wav"
        try:
            await asyncio.to_thread(record_chunk, radio, chunk)
            ring.append(chunk)
            transcript = await asyncio.to_thread(transcribe, chunk)
            for term in find_hits(transcript, terms):
                clip = await asyncio.to_thread(concat_clip, radio, list(ring))
                save_hit(radio, term, transcript, clip)
        except Exception as exc:  # noqa: BLE001
            upsert_worker(WORKER_ID, "radio", "warning", f"{radio['name']}: {exc}", {"radio_id": radio["id"]})
            await asyncio.sleep(10)


async def supervisor() -> None:
    init_db()
    while True:
        radios = active_radios()[:CONCURRENCY]
        if not radios:
            upsert_worker(WORKER_ID, "radio", "idle", "Nenhuma radio ativa com stream")
            await asyncio.sleep(30)
            continue
        upsert_worker(WORKER_ID, "radio", "running", f"Monitorando {len(radios)} radios")
        await asyncio.gather(*(monitor_radio(radio, index + 1) for index, radio in enumerate(radios)))


def main() -> None:
    asyncio.run(supervisor())


if __name__ == "__main__":
    main()
