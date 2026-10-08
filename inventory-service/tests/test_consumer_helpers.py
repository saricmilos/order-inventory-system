"""Tests for the pure helper in the consumer module.

``_extract_header`` pulls the correlation id out of Kafka message headers. The
heavy I/O parts of the consumer (poll loop, commit, DLQ) need a broker to test
end-to-end, but this header parsing is pure and worth covering directly.
"""

from app.consumer import _extract_header


def test_returns_none_when_headers_missing():
    assert _extract_header(None, "x-correlation-id") is None


def test_returns_none_for_empty_headers():
    assert _extract_header([], "x-correlation-id") is None


def test_decodes_bytes_value():
    headers = [("x-correlation-id", b"abc-123")]
    assert _extract_header(headers, "x-correlation-id") == "abc-123"


def test_returns_str_value_as_is():
    headers = [("x-correlation-id", "abc-123")]
    assert _extract_header(headers, "x-correlation-id") == "abc-123"


def test_returns_none_when_key_absent():
    headers = [("x-other-header", b"value")]
    assert _extract_header(headers, "x-correlation-id") is None


def test_finds_key_among_many_headers():
    headers = [
        ("x-first", b"1"),
        ("x-correlation-id", b"target"),
        ("x-last", b"3"),
    ]
    assert _extract_header(headers, "x-correlation-id") == "target"
