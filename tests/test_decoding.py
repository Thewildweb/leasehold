import base64

import pytest

import leasehold.decoding as d
from leasehold.decoding import LABELS, body_text, decode_body
from leasehold.responses import Response


def test_decode_body_charsets():
    latin = "café".encode("latin-1")
    assert decode_body(latin, "text/html; charset=ISO-8859-1", False) == ("café", "cp1252", 0)
    meta = b'<meta charset="windows-1252">' + latin
    assert decode_body(meta, "text/html", False)[1] == "cp1252"
    assert decode_body(b"caf\xc3\xa9", "text/html; charset=nonsense", False) == (
        "café", "utf-8", 0,
    )  # fmt: skip
    # Declared UTF-8 but not: replaced and counted, as a browser shows it.
    assert decode_body(latin + b"!", "text/html; charset=utf-8", False) == (
        "caf\ufffd!",
        "utf-8",
        1,
    )


def test_decode_body_undeclared_falls_back_to_windows_1252():
    """No charset anywhere: strict UTF-8, else windows-1252 (was UTF-8 with U+FFFD)."""
    assert decode_body("café".encode(), "text/html", False) == ("café", "utf-8", 0)
    assert decode_body("café €5".encode("cp1252"), "text/html", False) == ("café €5", "cp1252", 0)
    # A body cut inside a UTF-8 character is still UTF-8.
    assert decode_body("café".encode()[:-1], "text/html", True) == ("caf", "utf-8", 0)


def test_decode_body_bom_wins():
    assert decode_body(b"\xef\xbb\xbfcaf\xc3\xa9", "text/html; charset=iso-8859-1", False) == (
        "café", "utf-8", 0,
    )  # fmt: skip
    assert decode_body(b"\xff\xfe" + "ok".encode("utf-16-le"), "text/html", False) == (
        "ok", "utf-16-le", 0,
    )  # fmt: skip


@pytest.mark.parametrize("label", ["zlib", "base64", "hex", "rot13", "utf-7", "bogus"])
def test_decode_body_ignores_labels_browsers_dont_know(label):
    """Labels outside the WHATWG list are ignored (no Python codec registry lookup):
    no exception, and UTF-7 can't turn +ADw-script+AD4- into markup."""
    body = b"<p>+ADw-script+AD4-alert(1)+ADw-/script+AD4- caf\xc3\xa9</p>"
    assert decode_body(body, f"text/html; charset={label}", False) == (
        "<p>+ADw-script+AD4-alert(1)+ADw-/script+AD4- café</p>", "utf-8", 0
    )  # fmt: skip
    meta = b'<meta charset="' + label.encode() + b'"><p>caf\xe9</p>'
    assert decode_body(meta, "text/html", False) == (
        '<meta charset="' + label + '"><p>café</p>', "cp1252", 0
    )  # fmt: skip


@pytest.mark.parametrize("label", ["iso-2022-kr", "hz-gb-2312", "replacement", "x-user-defined"])
def test_decode_body_replacement_labels_are_ignored(label):
    assert decode_body(b"plain ascii", f"text/html; charset={label}", False) == (
        "plain ascii", "utf-8", 0
    )  # fmt: skip


@pytest.mark.parametrize(
    ("label", "codec"),
    [("ISO-8859-1", "cp1252"), ("ascii", "cp1252"), ("latin1", "cp1252"), ("US-ASCII", "cp1252"),
     ("gb2312", "gb18030"), ("GBK", "gb18030"), ("iso-8859-9", "cp1254"),
     ("tis-620", "cp874"), ("shift_jis", "cp932"), ("euc-kr", "cp949"), ("big5", "big5hkscs"),
     ("UTF8", "utf-8")],
)  # fmt: skip
def test_decode_body_whatwg_labels(label, codec):
    assert decode_body(b"abc", f"text/html; charset={label}", False)[1] == codec


def test_decode_body_gb2312_is_gb18030():
    """Python's narrow gb2312 replaces characters that GBK/gb18030 has."""
    text = "\u9555\u7881\u6911 \u4e2d\u6587"
    raw = text.encode("gb18030")
    assert decode_body(raw, "text/html; charset=gb2312", False) == (text, "gb18030", 0)


def test_decode_body_utf16_label_on_an_odd_length_body_does_not_raise():
    raw = "<html>odd".encode("utf-16-le") + b"!"
    text, charset, _ = decode_body(raw, "text/html; charset=utf-16", False)
    assert charset == "utf-16-le" and text.startswith("<html>odd")


def test_decode_body_truncated_multibyte_endings():
    """A cut body may end inside a character: not counted as a replacement."""
    for label, codec, s in (("utf-8", "utf-8", "café"), ("gbk", "gb18030", "\u4e2d\u6587"),
                            ("shift_jis", "cp932", "\u65e5\u672c"),
                            ("utf-16le", "utf-16-le", "ok")):  # fmt: skip
        raw = s.encode(codec)[:-1]
        text, charset, replaced = decode_body(raw, f"text/html; charset={label}", True)
        assert (text, charset, replaced) == (s[:-1], codec, 0)


def test_decode_body_never_raises(monkeypatch):
    f = d

    def boom(*a):
        raise LookupError("broken codec")

    monkeypatch.setattr(f, "_decode_body", boom)
    assert decode_body(b"caf\xc3\xa9 \xff", "text/html", False) == ("café \ufffd", "utf-8", 1)


def test_decode_body_utf16_label_needs_a_utf16_body():
    """A declared UTF-16 counts only when the body looks like UTF-16 (0x00 bytes)."""
    text = "<html>café</html>"
    assert decode_body(text.encode("utf-16-le"), "text/html; charset=utf-16", False) == (
        text, "utf-16-le", 0,
    )  # fmt: skip
    assert decode_body(text.encode("utf-16-be"), "text/html; charset=utf-16be", False) == (
        text, "utf-16-be", 0,
    )  # fmt: skip
    # An ASCII/UTF-8 page labelled utf-16: decoded as if nothing were declared.
    assert decode_body(text.encode(), "text/html; charset=utf-16", False) == (text, "utf-8", 0)
    # ... and the page's own <meta> still applies.
    meta = b'<meta charset="windows-1252"><p>caf\xe9</p>'
    assert decode_body(meta, "text/html; charset=utf-16", False)[1] == "cp1252"


def test_labels_cover_the_whatwg_list():
    assert len(LABELS) >= 200
    assert all(label == label.lower() for label in LABELS)
    for codec in set(LABELS.values()):
        assert decode_body(b"", f"text/html; charset={codec}", False)[0] == ""


def _resp(**kw) -> Response:
    return Response(
        domain="example.nl", url="https://example.nl/", target_ip="192.0.2.1", attempt=1,
        client="test", impersonate="", user_agent="ua", elapsed_ms=1.0, **kw,
    )  # fmt: skip


def test_body_text_prefers_the_raw_bytes():
    raw = "café".encode("cp1252")
    r = _resp(body="caf?", body_charset="ascii", body_b64=base64.b64encode(raw).decode(),
              content_type="text/html; charset=iso-8859-1")  # fmt: skip
    assert body_text(r) == ("café", "cp1252")


def test_body_text_when_the_producer_dropped_the_text():
    raw = "€ 5,-".encode()
    r = _resp(body="", body_text_dropped=True, body_b64=base64.b64encode(raw).decode(),
              content_type="text/html")  # fmt: skip
    assert body_text(r) == ("€ 5,-", "utf-8")


def test_body_text_without_bytes_or_with_bad_base64():
    assert body_text(_resp(body="hallo", body_charset="utf-8")) == ("hallo", "utf-8")
    assert body_text(_resp(body="hallo", body_charset="utf-8", body_b64="%%%")) == (
        "hallo", "utf-8",
    )  # fmt: skip
