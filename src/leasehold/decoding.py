"""Body bytes to text, the way browsers pick the charset.

One decoder for every reader of ``leasehold.responses.Response``: producers fill
``body`` with it, consumers that re-decode ``body_b64`` (classifiers, analysers,
re-analysis of archived responses) use it too, so all of them see the same text.

Order as in browsers: a byte order mark, the Content-Type charset, ``<meta charset>``
in the first 4 KB. Nothing declared: strict UTF-8, else windows-1252. Charset labels
follow the WHATWG Encoding Standard; Python's codec registry is never consulted.
``decode_body`` never raises.
"""

from __future__ import annotations

import base64
import binascii
import codecs
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from leasehold.responses import Response

META_CHARSET = re.compile(
    rb"""<meta[^>]{0,200}?charset\s*=\s*["']?\s*([A-Za-z0-9_.:-]{1,40})""", re.I
)


BOMS = ((b"\xef\xbb\xbf", "utf-8"), (b"\xff\xfe", "utf-16-le"), (b"\xfe\xff", "utf-16-be"))


# WHATWG Encoding Standard (encoding.spec.whatwg.org, "Encodings"): every label a
# browser accepts, mapped to the Python codec that decodes it the same way. Labels not
# listed are ignored, as browsers ignore them; Python's own codec registry is never
# consulted (it knows zlib, base64, rot13 and utf-7, which no browser decodes). The
# "replacement" encodings (iso-2022-kr, hz-gb-2312, ...) and x-user-defined are left
# out: their label is ignored and the body is decoded as if nothing were declared.
_WHATWG = {
    "utf-8": "unicode-1-1-utf-8 unicode11utf8 unicode20utf8 utf-8 utf8 x-unicode20utf8",
    "cp866": "866 cp866 csibm866 ibm866",
    "iso8859_2": "csisolatin2 iso-8859-2 iso-ir-101 iso8859-2 iso88592 iso_8859-2 "
    "iso_8859-2:1987 l2 latin2",
    "iso8859_3": "csisolatin3 iso-8859-3 iso-ir-109 iso8859-3 iso88593 iso_8859-3 "
    "iso_8859-3:1988 l3 latin3",
    "iso8859_4": "csisolatin4 iso-8859-4 iso-ir-110 iso8859-4 iso88594 iso_8859-4 "
    "iso_8859-4:1988 l4 latin4",
    "iso8859_5": "csisolatincyrillic cyrillic iso-8859-5 iso-ir-144 iso8859-5 iso88595 "
    "iso_8859-5 iso_8859-5:1988",
    "iso8859_6": "arabic asmo-708 csiso88596e csiso88596i csisolatinarabic ecma-114 "
    "iso-8859-6 iso-8859-6-e iso-8859-6-i iso-ir-127 iso8859-6 iso88596 iso_8859-6 "
    "iso_8859-6:1987",
    "iso8859_7": "csisolatingreek ecma-118 elot_928 greek greek8 iso-8859-7 iso-ir-126 "
    "iso8859-7 iso88597 iso_8859-7 iso_8859-7:1987 sun_eu_greek",
    "iso8859_8": "csiso88598e csisolatinhebrew hebrew iso-8859-8 iso-8859-8-e iso-ir-138 "
    "iso8859-8 iso88598 iso_8859-8 iso_8859-8:1988 visual csiso88598i iso-8859-8-i logical",
    "iso8859_10": "csisolatin6 iso-8859-10 iso-ir-157 iso8859-10 iso885910 l6 latin6",
    "iso8859_13": "iso-8859-13 iso8859-13 iso885913",
    "iso8859_14": "iso-8859-14 iso8859-14 iso885914",
    "iso8859_15": "csisolatin9 iso-8859-15 iso8859-15 iso885915 iso_8859-15 l9",
    "iso8859_16": "iso-8859-16",
    "koi8_r": "cskoi8r koi koi8 koi8-r koi8_r",
    "koi8_u": "koi8-ru koi8-u",
    "mac_roman": "csmacintosh mac macintosh x-mac-roman",
    "cp874": "dos-874 iso-8859-11 iso8859-11 iso885911 tis-620 windows-874",
    "cp1250": "cp1250 windows-1250 x-cp1250",
    "cp1251": "cp1251 windows-1251 x-cp1251",
    "cp1252": "ansi_x3.4-1968 ascii cp1252 cp819 csisolatin1 ibm819 iso-8859-1 iso-ir-100 "
    "iso8859-1 iso88591 iso_8859-1 iso_8859-1:1987 l1 latin1 us-ascii windows-1252 x-cp1252",
    "cp1253": "cp1253 windows-1253 x-cp1253",
    "cp1254": "cp1254 csisolatin5 iso-8859-9 iso-ir-148 iso8859-9 iso88599 iso_8859-9 "
    "iso_8859-9:1989 l5 latin5 windows-1254 x-cp1254",
    "cp1255": "cp1255 windows-1255 x-cp1255",
    "cp1256": "cp1256 windows-1256 x-cp1256",
    "cp1257": "cp1257 windows-1257 x-cp1257",
    "cp1258": "cp1258 windows-1258 x-cp1258",
    "mac_cyrillic": "x-mac-cyrillic x-mac-ukrainian",
    "gb18030": "chinese csgb2312 csiso58gb231280 gb2312 gb_2312 gb_2312-80 gbk iso-ir-58 "
    "x-gbk gb18030",
    "big5hkscs": "big5 big5-hkscs cn-big5 csbig5 x-x-big5",
    "euc_jp": "cseucpkdfmtjapanese euc-jp x-euc-jp",
    "iso2022_jp": "csiso2022jp iso-2022-jp",
    "cp932": "csshiftjis ms932 ms_kanji shift-jis shift_jis sjis windows-31j x-sjis",
    "cp949": "cseuckr csksc56011987 euc-kr iso-ir-149 korean ks_c_5601-1987 ks_c_5601-1989 "
    "ksc5601 ksc_5601 windows-949",
    "utf-16-be": "unicodefffe utf-16be",
    "utf-16-le": "csunicode iso-10646-ucs-2 ucs-2 unicode unicodefeff utf-16 utf-16le",
}
LABELS: dict[str, str] = {label: codec for codec, ls in _WHATWG.items() for label in ls.split()}

# A declared UTF-16 is only believed when the body looks like it: UTF-16 markup has a
# 0x00 byte for every ASCII character. A server that labels an ASCII page utf-16 would
# otherwise turn it into unreadable text.
UTF16_SNIFF_BYTES = 1024
UTF16_MIN_NUL_SHARE = 0.2


def charset_codec(label: str) -> str | None:
    """The Python codec for a charset label per the WHATWG list; None if not listed."""
    return LABELS.get(label.strip().lower())


def _looks_utf16(raw: bytes) -> bool:
    head = raw[:UTF16_SNIFF_BYTES]
    return bool(head) and head.count(0) / len(head) >= UTF16_MIN_NUL_SHARE


def decode_body(raw: bytes, content_type: str, truncated: bool) -> tuple[str, str, int]:
    """Text, the codec used, and how many characters were replaced (U+FFFD).

    ``truncated``: the body was cut at a size limit, so a multi-byte character split at
    the end is dropped rather than replaced. Never raises: on any error the body is
    decoded as UTF-8 with replacement characters."""
    try:
        return _decode_body(raw, content_type, truncated)
    except Exception:  # noqa: BLE001 - a body must never stop the event
        text = raw.decode("utf-8", errors="replace")
        return text, "utf-8", text.count("�")


def _decode_body(raw: bytes, content_type: str, truncated: bool) -> tuple[str, str, int]:
    for bom, name in BOMS:
        if raw.startswith(bom):
            return _decode(raw[len(bom) :], name, truncated)
    m = re.search(r"charset\s*=\s*[\"']?([A-Za-z0-9_.:-]+)", content_type or "", re.I)
    name = charset_codec(m.group(1)) if m else None
    if name and name.startswith("utf-16") and not _looks_utf16(raw):
        name = None
    if name is None:
        mm = META_CHARSET.search(raw[:4096])
        name = charset_codec(mm.group(1).decode("ascii", "ignore")) if mm else None
        if name and name.startswith("utf-16"):
            name = "utf-8"  # a document that reads its own <meta> as ASCII isn't UTF-16
    if name is None:
        try:
            # A cut body can end inside a multi-byte character: not final.
            text = codecs.getincrementaldecoder("utf-8")().decode(raw, final=not truncated)
            return text, "utf-8", 0
        except UnicodeDecodeError:
            name = "cp1252"
    return _decode(raw, name, truncated)


def _decode(raw: bytes, name: str, truncated: bool) -> tuple[str, str, int]:
    decoder = codecs.getincrementaldecoder(name)(errors="replace")
    # A cut body can end inside a multi-byte character: not final, so it isn't replaced.
    text = decoder.decode(raw, final=not truncated)
    return text, name, text.count("�")


def body_text(resp: Response) -> tuple[str, str]:
    """(text, codec) of a response's body. Decoded from ``body_b64`` when the event has
    the bytes, so every reader gets the same text whatever the producer put in ``body``
    (and whether or not it dropped the text to fit the size limit); else ``body`` as
    sent. Invalid base64 falls back to ``body``."""
    if resp.body_b64:
        try:
            raw = base64.b64decode(resp.body_b64, validate=True)
        except (binascii.Error, ValueError):
            return resp.body, resp.body_charset
        text, codec, _ = decode_body(raw, resp.content_type, resp.body_truncated)
        return text, codec
    return resp.body, resp.body_charset
