from __future__ import annotations

import asyncio
import os
import re
import shutil
import socket
import subprocess
import time
from datetime import datetime, timedelta, timezone
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


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def save_segment(radio: dict, chunk: Path, started_at: str, ended_at: str) -> int:
    with connect() as conn:
        cursor = conn.execute(
            """
            insert into radio_segments
            (radio_id, radio_name, city, audio_path, status, started_at, ended_at, created_at, updated_at)
            values (?, ?, ?, ?, 'recorded', ?, ?, ?, ?)
            """,
            (
                radio["id"],
                radio["name"],
                radio.get("city", ""),
                str(chunk),
                started_at,
                ended_at,
                utc_now(),
                utc_now(),
            ),
        )
        return int(cursor.lastrowid)


def update_segment(segment_id: int, status: str, transcript: str = "", terms: list[str] | None = None, error: str = "") -> None:
    with connect() as conn:
        conn.execute(
            """
            update radio_segments
            set status = ?, transcript = ?, matched_terms = ?, error = ?, updated_at = ?
            where id = ?
            """,
            (status, transcript[:2400], ", ".join(terms or []), error[:800], utc_now(), segment_id),
        )


def pending_segments(limit: int = 20) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            """
            select * from radio_segments
            where status = 'recorded'
            order by created_at asc
            limit ?
            """,
            (limit,),
        ).fetchall()
    segments = rows_to_dicts(rows)
    shard_radio_ids = {radio["id"] for radio in active_radios()}
    return [segment for segment in segments if segment["radio_id"] in shard_radio_ids]


def reset_stale_processing() -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    with connect() as conn:
        conn.execute(
            """
            update radio_segments
            set status = 'recorded', error = 'Retornado para fila apos reinicio/timeout', updated_at = ?
            where status = 'processing' and updated_at < ?
            """,
            (utc_now(), cutoff),
        )


def clip_segments(radio_id: int, center_started_at: str, center_ended_at: str) -> list[tuple[Path, str, str]]:
    start_window = parse_iso(center_started_at) - timedelta(seconds=BEFORE_SECONDS)
    end_window = parse_iso(center_ended_at) + timedelta(seconds=AFTER_SECONDS)
    with connect() as conn:
        rows = conn.execute(
            """
            select audio_path, started_at, ended_at from radio_segments
            where radio_id = ?
              and started_at <= ?
              and ended_at >= ?
            order by started_at asc
            """,
            (radio_id, end_window.isoformat(), start_window.isoformat()),
        ).fetchall()
    chunks: list[tuple[Path, str, str]] = []
    for row in rows:
        path = Path(row["audio_path"])
        if path.exists():
            chunks.append((path, row["started_at"], row["ended_at"]))
    return chunks


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
    last_recorded_at = ended_at if status in {"recorded", "backlog"} else ""
    last_transcribed_at = ended_at if status == "transcribed" else ""
    with connect() as conn:
        conn.execute(
            """
            insert into radio_checks
            (radio_id, radio_name, city, status, transcript, matched_terms, last_audio_path, last_recorded_at, last_transcribed_at, started_at, ended_at, error, updated_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(radio_id) do update set
              radio_name=excluded.radio_name,
              city=excluded.city,
              status=excluded.status,
              transcript=case when excluded.transcript != '' then excluded.transcript else radio_checks.transcript end,
              matched_terms=case
                when excluded.status = 'transcribed' then excluded.matched_terms
                when excluded.matched_terms != '' then excluded.matched_terms
                else radio_checks.matched_terms
              end,
              last_audio_path=excluded.last_audio_path,
              last_recorded_at=case when excluded.last_recorded_at != '' then excluded.last_recorded_at else radio_checks.last_recorded_at end,
              last_transcribed_at=case when excluded.last_transcribed_at != '' then excluded.last_transcribed_at else radio_checks.last_transcribed_at end,
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
                last_recorded_at,
                last_transcribed_at,
                started_at,
                ended_at,
                error[:500],
                utc_now(),
            ),
        )


async def finalize_hit_clip(radio: dict, term: str, transcript: str, center_started_at: str, center_ended_at: str) -> None:
    await asyncio.sleep(AFTER_SECONDS + 3)
    chunks = await asyncio.to_thread(clip_segments, radio["id"], center_started_at, center_ended_at)
    if not chunks:
        save_check(
            radio,
            "warning",
            transcript,
            [term],
            started_at=center_started_at,
            ended_at=center_ended_at,
            error="Termo detectado, mas nenhum bloco de audio ficou disponivel para montar o clipe.",
        )
        return
    clip = await asyncio.to_thread(concat_clip, radio, chunks)
    save_hit(radio, term, transcript, clip, chunks[0][1], chunks[-1][2])


async def analyze_chunk(
    radio: dict,
    radio_worker_id: str,
    segment_id: int,
    chunk_meta: tuple[Path, str, str],
    transcribe_gate: asyncio.Semaphore,
    last_hits: dict[str, float],
) -> None:
    terms = active_terms()
    try:
        async with transcribe_gate:
            transcript = await asyncio.to_thread(transcribe, chunk_meta[0])
        hits = matched_terms(transcript, terms, radio_mode=True)
        update_segment(segment_id, "matched" if hits else "transcribed", transcript, hits)
        probe_path = save_probe_audio(radio, chunk_meta[0])
        save_check(radio, "transcribed", transcript, hits, probe_path, chunk_meta[1], chunk_meta[2])
        for term in hits:
            if time.time() - last_hits.get(term, 0) < HIT_COOLDOWN_SECONDS:
                continue
            last_hits[term] = time.time()
            upsert_worker(radio_worker_id, "radio", "capturing", f"{radio['name']}: {term}", {"radio_id": radio["id"]})
            asyncio.create_task(finalize_hit_clip(radio, term, transcript, chunk_meta[1], chunk_meta[2]))
    except Exception as exc:  # noqa: BLE001
        update_segment(segment_id, "error", error=str(exc))
        save_check(radio, "warning", error=str(exc), started_at=chunk_meta[1], ended_at=chunk_meta[2])
        upsert_worker(radio_worker_id, "radio", "warning", f"{radio['name']}: {exc}", {"radio_id": radio["id"]})


async def monitor_radio(radio: dict, slot: int, transcribe_gate: asyncio.Semaphore) -> None:
    slug = safe_slug(f"{radio['name']}-{radio.get('city', '')}")
    radio_worker_id = f"{WORKER_ID}-radio-{radio['id']}"
    last_hits: dict[str, float] = {}
    while True:
        upsert_worker(radio_worker_id, "radio", "running", f"{radio['name']} ({slot})", {"radio_id": radio["id"]})
        chunk = BUFFER / slug / f"{int(time.time())}.wav"
        try:
            started_at, ended_at = await asyncio.to_thread(record_chunk, radio, chunk)
            chunk_meta = (chunk, started_at, ended_at)
            segment_id = save_segment(radio, chunk, started_at, ended_at)
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
                analyze_chunk(radio, radio_worker_id, segment_id, chunk_meta, transcribe_gate, last_hits)
            )
            update_segment(segment_id, "processing")
            _PENDING_TRANSCRIPTS.add(task)
            task.add_done_callback(_PENDING_TRANSCRIPTS.discard)
        except Exception as exc:  # noqa: BLE001
            save_check(radio, "warning", error=str(exc))
            upsert_worker(radio_worker_id, "radio", "warning", f"{radio['name']}: {exc}", {"radio_id": radio["id"]})
            await asyncio.sleep(10)


async def recover_recorded_segments(transcribe_gate: asyncio.Semaphore) -> None:
    last_hits_by_radio: dict[int, dict[str, float]] = {}
    while True:
        try:
            reset_stale_processing()
            available_slots = MAX_PENDING_TRANSCRIPTS - len(_PENDING_TRANSCRIPTS)
            if available_slots <= 0:
                await asyncio.sleep(5)
                continue
            for segment in pending_segments(min(available_slots, 10)):
                radio = {
                    "id": segment["radio_id"],
                    "name": segment["radio_name"],
                    "city": segment.get("city", ""),
                }
                update_segment(segment["id"], "processing")
                chunk_meta = (Path(segment["audio_path"]), segment["started_at"], segment["ended_at"])
                task = asyncio.create_task(
                    analyze_chunk(
                        radio,
                        f"{WORKER_ID}-recovery-{segment['radio_id']}",
                        segment["id"],
                        chunk_meta,
                        transcribe_gate,
                        last_hits_by_radio.setdefault(segment["radio_id"], {}),
                    )
                )
                _PENDING_TRANSCRIPTS.add(task)
                task.add_done_callback(_PENDING_TRANSCRIPTS.discard)
        except Exception as exc:  # noqa: BLE001
            upsert_worker(WORKER_ID, "radio", "warning", f"Recuperacao de fila: {exc}")
        await asyncio.sleep(5)


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
        await asyncio.gather(
            recover_recorded_segments(transcribe_gate),
            *(monitor_radio(radio, index + 1, transcribe_gate) for index, radio in enumerate(radios)),
        )


def main() -> None:
    asyncio.run(supervisor())


if __name__ == "__main__":
    main()

