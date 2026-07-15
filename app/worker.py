from __future__ import annotations

import asyncio
import os
import re
import shutil
import socket
import subprocess
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from .db import connect, init_db, rows_to_dicts, upsert_worker, utc_now
from .matching import matched_terms


ROOT = Path(__file__).resolve().parents[1]
BUFFER = ROOT / "storage" / "buffer"
AUDIO = ROOT / "storage" / "audio"
WORKER_ID = os.environ.get("RADAR_WORKER_ID", f"radio-worker-{socket.gethostname()}-{os.getpid()}")
MAX_RADIOS = int(os.environ.get("RADAR_MAX_RADIOS", os.environ.get("RADAR_CONCURRENCY", "0")))
TRANSCRIBE_CONCURRENCY = int(os.environ.get("RADAR_TRANSCRIBE_CONCURRENCY", "2"))
SHARD_INDEX = int(os.environ.get("RADAR_SHARD_INDEX", "0"))
SHARD_COUNT = max(1, int(os.environ.get("RADAR_SHARD_COUNT", "1")))
MAX_PENDING_TRANSCRIPTS = int(os.environ.get("RADAR_MAX_PENDING_TRANSCRIPTS", "120"))
CHUNK_SECONDS = int(os.environ.get("RADAR_CHUNK_SECONDS", "30"))
BEFORE_SECONDS = int(os.environ.get("RADAR_BEFORE_SECONDS", "120"))
AFTER_SECONDS = int(os.environ.get("RADAR_AFTER_SECONDS", "120"))
BEFORE_CHUNKS = max(1, BEFORE_SECONDS // CHUNK_SECONDS)
AFTER_CHUNKS = max(1, AFTER_SECONDS // CHUNK_SECONDS)
HIT_COOLDOWN_SECONDS = int(os.environ.get("RADAR_HIT_COOLDOWN_SECONDS", "240"))
MODEL_NAME = os.environ.get("RADAR_WHISPER_MODEL", "tiny")
_MODEL = None
_PENDING_TRANSCRIPTS: set[asyncio.Task] = set()


def safe_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "radio"


def ffmpeg_path() -> str:
    exe = shutil.which("ffmpeg")
    if not exe:
        raise RuntimeError("ffmpeg não encontrado no servidor")
    return exe


def active_terms() -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "select * from terms where status != 'Pausado' and group_name != 'Negativo'"
        ).fetchall()
    return rows_to_dicts(rows)


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
    radios = rows_to_dicts(rows)
    return [radio for index, radio in enumerate(radios) if index % SHARD_COUNT == SHARD_INDEX]


def terms_prompt() -> str:
    terms = [term["term"] for term in active_terms() if term.get("term")]
    if not terms:
        return ""
    return "Termos importantes para reconhecer: " + ", ".join(terms[:30])


def transcribe(path: Path) -> str:
    global _MODEL
    try:
        from faster_whisper import WhisperModel
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("faster-whisper não instalado no worker") from exc

    if _MODEL is None:
        _MODEL = WhisperModel(MODEL_NAME, device="cpu", compute_type="int8")
    segments, _ = _MODEL.transcribe(str(path), language="pt", initial_prompt=terms_prompt())
    return " ".join(segment.text.strip() for segment in segments).strip()


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


def save_probe_audio(radio: dict, chunk: Path) -> str:
    AUDIO.mkdir(parents=True, exist_ok=True)
    slug = safe_slug(f"probe-{radio['id']}-{radio['name']}-{radio.get('city', '')}")
    output = AUDIO / f"{slug}.wav"
    shutil.copyfile(chunk, output)
    return f"/audio/{output.name}"


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


def save_check(
    radio: dict,
    status: str,
    transcript: str = "",
    terms: list[str] | None = None,
    last_audio_path: str = "",
    started_at: str = "",
    ended_at: str = "",
    error: str = "",
) -> None:
    with connect() as conn:
        conn.execute(
            """
            insert into radio_checks
            (radio_id, radio_name, city, status, transcript, matched_terms, last_audio_path, started_at, ended_at, error, updated_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(radio_id) do update set
              radio_name=excluded.radio_name,
              city=excluded.city,
              status=excluded.status,
              transcript=excluded.transcript,
              matched_terms=excluded.matched_terms,
              last_audio_path=excluded.last_audio_path,
              started_at=excluded.started_at,
              ended_at=excluded.ended_at,
              error=excluded.error,
              updated_at=excluded.updated_at
            """,
            (
                radio["id"],
                radio["name"],
                radio.get("city", ""),
                status,
                transcript[:1200],
                ", ".join(terms or []),
                last_audio_path,
                started_at,
                ended_at,
                error[:500],
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


async def analyze_chunk(
    radio: dict,
    radio_worker_id: str,
    ring_snapshot: list[tuple[Path, str, str]],
    chunk_meta: tuple[Path, str, str],
    slug: str,
    transcribe_gate: asyncio.Semaphore,
    last_hits: dict[str, float],
) -> None:
    terms = active_terms()
    try:
        async with transcribe_gate:
            transcript = await asyncio.to_thread(transcribe, chunk_meta[0])
        hits = matched_terms(transcript, terms, radio_mode=True)
        probe_path = save_probe_audio(radio, chunk_meta[0])
        save_check(radio, "transcribed", transcript, hits, probe_path, chunk_meta[1], chunk_meta[2])
        for term in hits:
            if time.time() - last_hits.get(term, 0) < HIT_COOLDOWN_SECONDS:
                continue
            last_hits[term] = time.time()
            upsert_worker(radio_worker_id, "radio", "capturing", f"{radio['name']}: {term}", {"radio_id": radio["id"]})
            after = await record_after_window(radio, slug)
            clip_chunks = ring_snapshot + after
            clip = await asyncio.to_thread(concat_clip, radio, clip_chunks)
            save_hit(radio, term, transcript, clip, clip_chunks[0][1], clip_chunks[-1][2])
    except Exception as exc:  # noqa: BLE001
        save_check(radio, "warning", error=str(exc), started_at=chunk_meta[1], ended_at=chunk_meta[2])
        upsert_worker(radio_worker_id, "radio", "warning", f"{radio['name']}: {exc}", {"radio_id": radio["id"]})


async def monitor_radio(radio: dict, slot: int, transcribe_gate: asyncio.Semaphore) -> None:
    slug = safe_slug(f"{radio['name']}-{radio.get('city', '')}")
    radio_worker_id = f"{WORKER_ID}-radio-{radio['id']}"
    ring: deque[tuple[Path, str, str]] = deque(maxlen=BEFORE_CHUNKS + 1)
    last_hits: dict[str, float] = {}
    while True:
        upsert_worker(radio_worker_id, "radio", "running", f"{radio['name']} ({slot})", {"radio_id": radio["id"]})
        chunk = BUFFER / slug / f"{int(time.time())}.wav"
        try:
            started_at, ended_at = await asyncio.to_thread(record_chunk, radio, chunk)
            chunk_meta = (chunk, started_at, ended_at)
            ring.append(chunk_meta)
            probe_path = save_probe_audio(radio, chunk)
            save_check(radio, "recorded", last_audio_path=probe_path, started_at=started_at, ended_at=ended_at)
            if len(_PENDING_TRANSCRIPTS) >= MAX_PENDING_TRANSCRIPTS:
                save_check(
                    radio,
                    "backlog",
                    started_at=started_at,
                    ended_at=ended_at,
                    error=f"Fila de transcrição cheia: {len(_PENDING_TRANSCRIPTS)} pendentes",
                )
                continue
            task = asyncio.create_task(
                analyze_chunk(radio, radio_worker_id, list(ring), chunk_meta, slug, transcribe_gate, last_hits)
            )
            _PENDING_TRANSCRIPTS.add(task)
            task.add_done_callback(_PENDING_TRANSCRIPTS.discard)
        except Exception as exc:  # noqa: BLE001
            save_check(radio, "warning", error=str(exc))
            upsert_worker(radio_worker_id, "radio", "warning", f"{radio['name']}: {exc}", {"radio_id": radio["id"]})
            await asyncio.sleep(10)


async def supervisor() -> None:
    init_db()
    while True:
        radios = active_radios()
        if MAX_RADIOS > 0:
            radios = radios[:MAX_RADIOS]
        if not radios:
            upsert_worker(WORKER_ID, "radio", "idle", "Nenhuma rádio ativa com stream")
            await asyncio.sleep(30)
            continue
        upsert_worker(
            WORKER_ID,
            "radio",
            "running",
            f"Monitorando {len(radios)} rádios no shard {SHARD_INDEX + 1}/{SHARD_COUNT}",
            {"pending_transcripts": len(_PENDING_TRANSCRIPTS), "shard_index": SHARD_INDEX, "shard_count": SHARD_COUNT},
        )
        transcribe_gate = asyncio.Semaphore(max(1, TRANSCRIBE_CONCURRENCY))
        await asyncio.gather(*(monitor_radio(radio, index + 1, transcribe_gate) for index, radio in enumerate(radios)))


def main() -> None:
    asyncio.run(supervisor())


if __name__ == "__main__":
    main()

