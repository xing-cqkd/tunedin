"""Tests for backend.eval.audio (byte-range fetch + decode + transcribe)."""
from unittest.mock import MagicMock, patch

import numpy as np

from backend.eval.audio import (
    decode_to_pcm,
    fetch_and_transcribe,
    fetch_window_bytes,
    transcribe_pcm,
    window_pcm,
)


def _resp(status, content, headers=None):
    r = MagicMock()
    r.status_code = status
    r.content = content
    r.headers = dict(headers or {})
    return r


def test_fetch_window_bytes_range_header():
    with patch("backend.eval.audio.requests.get") as g:
        g.return_value = _resp(206, b"data")
        data, offset = fetch_window_bytes("http://x/y.mp3", 60.0, 90.0,
                                          total_bytes=3_600_000,
                                          duration=3600.0)
        assert data == b"data"
        _, kwargs = g.call_args
        assert kwargs["headers"]["Range"].startswith("bytes=")
        # offset accounts for the pre-roll
        off = int(kwargs["headers"]["Range"].split("=")[1].split("-")[0])
        assert off == int(60.0 * 1000) - 16384
        assert offset == round(60.0 - off / 1000, 3)


def test_fetch_window_bytes_honors_content_range():
    # server clamps the range: offset must follow the ACTUAL start byte
    with patch("backend.eval.audio.requests.get") as g:
        g.return_value = _resp(206, b"data",
                               {"Content-Range": "bytes 1000-2000/3600000"})
        _, offset = fetch_window_bytes("http://x/y.mp3", 60.0, 90.0,
                                       total_bytes=3_600_000,
                                       duration=3600.0)
        assert offset == round(60.0 - 1000 / 1000, 3)


def test_fetch_window_bytes_ignores_range_gracefully():
    # server returns 200 + full file: slice from start_s directly
    with patch("backend.eval.audio.requests.get") as g:
        g.return_value = _resp(200, b"data")
        data, offset = fetch_window_bytes("http://x/y.mp3", 60.0, 90.0,
                                          total_bytes=3_600_000,
                                          duration=3600.0)
        assert data == b"data"
        assert offset == 60.0


def test_fetch_window_bytes_failures_are_none():
    with patch("backend.eval.audio.requests.get") as g:
        g.return_value = _resp(404, b"")
        assert fetch_window_bytes("http://x", 0, 90, 1000, 100) == (None, 0.0)
    assert fetch_window_bytes("http://x", 0, 90, 0, 100) == (None, 0.0)
    assert fetch_window_bytes("http://x", 0, 0, 1000, 100) == (None, 0.0)


def test_window_pcm_trims_preroll():
    pcm = np.zeros(16000 * 100, dtype=np.float32)
    w = window_pcm(pcm, 90.0, slice_offset_s=1.5)
    assert len(w) == 16000 * 90
    # explicit offset (e.g. range-ignoring server): slice from start_s
    w2 = window_pcm(pcm, 90.0, slice_offset_s=60.0)
    assert len(w2) == 16000 * 40  # only 40s remain after the offset


def test_decode_to_pcm_failure_is_none():
    with patch("backend.eval.audio.subprocess.run",
               side_effect=FileNotFoundError):
        assert decode_to_pcm(b"junk") is None


def test_transcribe_pcm_short_audio_skipped():
    pcm = np.zeros(16000 * 5, dtype=np.float32)
    assert transcribe_pcm(pcm, MagicMock()) == ("", [])


def test_transcribe_pcm_word_timestamps():
    pcm = np.zeros(16000 * 20, dtype=np.float32)
    wd = MagicMock()
    wd.word, wd.start, wd.end = " hello ", 1.0, 1.5
    seg = MagicMock()
    seg.text = " hello world "
    seg.words = [wd]
    model = MagicMock()
    model.transcribe.return_value = ([seg], None)
    with patch("wave.open"), \
         patch("tempfile.NamedTemporaryFile") as tf, \
         patch("os.unlink"):
        tf.return_value.__enter__.return_value.name = "/tmp/x.wav"
        text, words = transcribe_pcm(pcm, model)
    assert text == "hello world"
    assert words == [("hello", 1.0, 1.5)]


def test_fetch_and_transcribe_short_circuits_on_fetch_failure():
    with patch("backend.eval.audio.fetch_window_bytes",
               return_value=(None, 0.0)):
        assert fetch_and_transcribe("http://x", 0, 90, 100, 100,
                                    MagicMock()) == ("", [])
