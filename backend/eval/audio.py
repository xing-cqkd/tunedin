"""Audio window fetching for the eval harness (XIN-143 Tier 2).

Byte-range fetching keeps transcription cost proportional to the windows
actually sampled: a 90s window costs ~90s of audio, not the whole file.
Pattern follows the tunedin-extract structure profiler: estimate bytes/sec
from content-length/duration, fetch the window plus a small pre-roll (MP3
frames need a run-in to decode cleanly), decode with ffmpeg, trim the
pre-roll, slice to the window.

The faster-whisper model object is injected by the caller (the Tier 2
runner owns the model path); this module never imports it, so unit tests
can stub the fetch/decode boundary.
"""
import re
import subprocess

import requests

_PRE_ROLL_BYTES = 16384
_PRE_ROLL_S = 1.5
_SAMPLE_RATE = 16000


def _byte_offset(start_s: float, total_bytes: int,
                 duration: float) -> int:
    bps = total_bytes / duration
    return max(0, int(start_s * bps) - _PRE_ROLL_BYTES)


def fetch_window_bytes(audio_url: str, start_s: float, dur_s: float,
                       total_bytes: int, duration: float,
                       timeout: int = 30) -> tuple[bytes | None, float]:
    """Fetch the byte range covering [start_s, start_s+dur_s].

    Returns (data, slice_offset_s): seconds into the decoded PCM where the
    requested window starts. ``(None, 0.0)`` on any network failure
    (callers treat it as a skip, the same best-effort policy as the
    100-episode audio scan). Handles servers that ignore Range (200 with
    the full file) by slicing from start_s directly.
    """
    if total_bytes <= 0 or duration <= 0 or dur_s <= 0:
        return None, 0.0
    bps = total_bytes / duration
    off = _byte_offset(start_s, total_bytes, duration)
    nbytes = int(bps * (dur_s + _PRE_ROLL_S + 2)) + _PRE_ROLL_BYTES
    try:
        r = requests.get(audio_url,
                         headers={"Range": f"bytes={off}-{off + nbytes - 1}",
                                  "User-Agent": "Mozilla/5.0"},
                         timeout=timeout)
        if not r.content:
            return None, 0.0
        if r.status_code == 206:
            # Data starts at the ACTUAL range start (servers may clamp or
            # shift the requested range); the window starts at start_s.
            actual_off = off
            headers = r.headers or {}
            cr = headers.get("Content-Range", "") if hasattr(headers, "get") else ""
            if isinstance(cr, str):
                m = re.match(r"bytes (\d+)-", cr)
                if m:
                    actual_off = int(m.group(1))
            return r.content, max(0.0, start_s - actual_off / bps)
        if r.status_code == 200:
            # server ignored Range: full file, slice from start_s
            return r.content, start_s
        return None, 0.0
    except requests.RequestException:
        return None, 0.0


def decode_to_pcm(data: bytes):
    """MP3 bytes -> mono 16kHz float32 PCM (numpy array), or None."""
    import numpy as np
    try:
        p = subprocess.run(
            ["ffmpeg", "-v", "error", "-i", "pipe:0", "-ac", "1", "-ar",
             str(_SAMPLE_RATE), "-f", "f32le", "pipe:1"],
            input=data, capture_output=True, timeout=60)
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return None
    if p.returncode != 0 or not p.stdout:
        return None
    pcm = np.frombuffer(p.stdout, dtype=np.float32)
    return pcm if len(pcm) > _SAMPLE_RATE * 5 else None


def window_pcm(pcm, dur_s: float, slice_offset_s: float):
    """Slice exactly dur_s seconds of PCM starting at slice_offset_s.

    slice_offset_s is required (no default): it must come from
    fetch_window_bytes' return value, which derives the actual byte
    pre-roll from the response. A stale default here once shifted every
    window by a fixed 1.5s.
    """
    import numpy as np  # noqa: F401  (documents the array contract)
    skip = int(_SAMPLE_RATE * slice_offset_s)
    return pcm[skip:skip + int(_SAMPLE_RATE * dur_s)]


def transcribe_pcm(pcm, model) -> tuple[str, list[tuple]]:
    """PCM -> (text, [(word, start_s, end_s)]) with word timestamps.

    Word-level timestamps are the XIN-144 lever: they position
    text-detected phrases (ad reads, quotes) inside the audio, which is
    what turns a transcript takeaway into a *timestamped* takeaway.
    """
    import numpy as np
    import os
    import tempfile
    import wave
    if pcm is None or len(pcm) < _SAMPLE_RATE * 10:
        return "", []
    pcm16 = (np.clip(pcm, -1, 1) * 32767).astype(np.int16)
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
        path = f.name
    try:
        with wave.open(path, "wb") as w:
            w.setnchannels(1)
            w.setsampwidth(2)
            w.setframerate(_SAMPLE_RATE)
            w.writeframes(pcm16.tobytes())
        segments, _ = model.transcribe(path, beam_size=1,
                                       word_timestamps=True)
        text_parts, words = [], []
        for s in segments:
            text_parts.append(s.text.strip())
            for wd in (getattr(s, "words", None) or []):
                words.append((wd.word.strip(), round(wd.start, 2),
                              round(wd.end, 2)))
        return " ".join(text_parts), words
    except Exception:
        return "", []
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def fetch_and_transcribe(audio_url: str, start_s: float, dur_s: float,
                         total_bytes: int, duration: float,
                         model) -> tuple[str, list[tuple]]:
    """One call: fetch a window and transcribe it. ("", []) on failure."""
    data, slice_offset_s = fetch_window_bytes(audio_url, start_s, dur_s,
                                              total_bytes, duration)
    if data is None:
        return "", []
    pcm = decode_to_pcm(data)
    if pcm is None:
        return "", []
    return transcribe_pcm(window_pcm(pcm, dur_s, slice_offset_s), model)
