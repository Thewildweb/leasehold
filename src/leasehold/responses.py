"""The response event: one fetch attempt, as every scraping service reports it.

Every service that requests pages from sites (a first-page fetcher, a browser, a
scraper) emits this payload, or a subclass that adds its own fields, on its
own ``responses`` topic. The response classifier reads those topics and depends on
this model only, never on a service's package. A change here is a new
``schema_version``.

One event per task attempt. An attempt can make more than one request: the first
request, a fallback to http when https fails before any response, the redirects it
follows (manually, so every hop is recorded), and one retry without certificate
verification when verification failed. Every request is a ``Hop``, in order; the
top-level fields describe the last hop, so most readers never look at ``hops``.

Fields a client cannot capture stay at their defaults (curl: no certificate SAN list,
no HTTP/2 GOAWAY or RST_STREAM codes). A client that pins the request to the leased
IP sees ``remote_ip == target_ip`` by construction on those hops.

Envelope: ``target`` = ``target_ip`` (the lease target), ``egress_ip`` = the address
the requests leave from, ``task_key`` = the service's task key. Message key = domain.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

FailureStage = Literal[
    "none",  # the request got a complete response (or was cut off on purpose)
    "dns",  # name resolution (curl 6); only for hosts other than the pinned one
    "connect",  # TCP connect: refused (7), timed out before connecting (28)
    "tls",  # handshake or certificate (35, 53-60, 64-66, 77, 80-83, 90, 91)
    "send",  # sending the request (55)
    "headers",  # waiting for or reading the status line and headers (52, 56, 92, 28)
    "body",  # after the headers, before the body was complete or cut off
    "policy",  # not sent or not followed by the worker's own rules (see ``stop_reason``)
]

HttpVersion = Literal["", "1.0", "1.1", "2", "3"]


class Hop(BaseModel):
    """One HTTP request of an attempt."""

    url: str
    method: Literal["GET"] = "GET"
    verify: bool = Field(description="Certificate verification was on for this request")
    status: int = Field(default=0, description="0: no status line received")
    http_version: HttpVersion = ""
    remote_ip: str = Field(default="", description="Address actually connected to")
    remote_port: int = 0
    reused_connection: bool = Field(
        default=False, description="Connection kept from the previous hop: no connect/TLS times"
    )
    # curl's cumulative times from the start of this request, in ms (NAMELOOKUP,
    # CONNECT, APPCONNECT, STARTTRANSFER, TOTAL); None: stage not reached. On a reused
    # connection dns/connect/tls are 0.
    dns_ms: float | None = None
    connect_ms: float | None = None
    tls_ms: float | None = None
    ttfb_ms: float | None = None
    total_ms: float | None = None
    failure_stage: FailureStage = "none"
    error_code: int = Field(default=0, description="curl CURLE_* code from the client; 0: none")
    error_name: str = Field(
        default="",
        description="Symbolic name: CURLE_OPERATION_TIMEDOUT, or net::ERR_* for browsers",
    )
    error: str = Field(default="", description="The client's message, at most 300 characters")
    tls_version: str = Field(default="", description="e.g. TLSv1.3; '' for http")
    tls_cipher: str = ""
    cert_subject: str = ""
    cert_issuer: str = ""
    cert_not_before: str = Field(default="", description="ISO 8601 UTC")
    cert_not_after: str = Field(default="", description="ISO 8601 UTC")
    tls_verify: str = Field(
        default="",
        description="With verification on: 'ok', or curl's reason when it failed "
        "(expired, self-signed, name mismatch, unknown issuer). '' when not checked: "
        "http, or the retry with verification off",
    )
    headers: list[tuple[str, str]] = Field(
        default_factory=list,
        description="Response headers in the order received, duplicates kept (one "
        "Set-Cookie per cookie). Names as sent: HTTP/1 name case is a server-stack signal, "
        "readers lower-case them. Values cut at 2048 characters. A line is decoded as "
        "UTF-8, or as Latin-1 when it isn't valid UTF-8 (latin1_headers)",
    )
    latin1_headers: list[str] = Field(
        default_factory=list,
        description="Names of the headers decoded as Latin-1 because they weren't valid "
        "UTF-8 (':status' for the status line); value.encode('latin-1') gives the bytes "
        "sent. Every other header's bytes are value.encode('utf-8')",
    )
    location: str = Field(default="", description="Location header, resolved to an absolute URL")
    wire_bytes: int = Field(
        default=0,
        description="Body bytes as transferred, before content decoding (compressed size "
        "when the server compressed); 0 if unknown",
    )


class Response(BaseModel):
    """One attempt to fetch a page.

    The envelope carries ``target`` = the task's IP and ``egress_ip`` = the address
    this worker's requests leave from.
    """

    schema_version: int = 1

    # What was asked
    domain: str = Field(description="Registered domain of the site")
    url: str = Field(description="First URL requested")
    target_ip: str = Field(description="The IP the task was leased for (from the DNS stage)")
    attempt: int = Field(description="1 for the first attempt of the task")
    role: Literal["scrape", "probe"] = "scrape"
    retry_of: str = Field(
        default="",
        description="Event ID of the response this request immediately retries with other "
        "client settings; '' for a first request",
    )
    retry_reason: str = Field(default="", description="Why the retry was made; '' if none")

    # Who asked, how
    client: str = Field(description="e.g. curl_cffi 0.17.0")
    impersonate: str = Field(description="Browser profile the client imitates, e.g. chrome136")
    egress: str = Field(
        default="",
        description="Egress class or pool the task asked for; '' if none. The address used "
        "is the envelope's egress_ip",
    )
    user_agent: str = Field(description="User-Agent header sent")
    rules_version: str = Field(
        default="", description="Version of the reflex rules the worker applied"
    )
    ip_requests_before: int = Field(
        default=0,
        description="Requests this worker sent to target_ip before this attempt, while "
        "holding the IP's group (resets when the worker lets the IP go)",
    )
    cookies_before: int = Field(
        default=0, description="Cookies held before the first request; 0: fresh handle per task"
    )

    # Outcome of the last hop
    status: int = Field(default=0, description="Status of the last hop; 0: no response")
    final_url: str = ""
    http_version: HttpVersion = ""
    remote_ip: str = ""
    remote_port: int = 0
    failure_stage: FailureStage = "none"
    error_code: int = 0
    error_name: str = ""
    error: str = ""
    stop_reason: str = Field(
        default="",
        description="Why the worker stopped following redirects: '' (not a redirect), "
        "'offsite' (Location outside the base domain), 'max_hops', 'loop', 'bad_location'",
    )
    https_fallback: bool = Field(
        default=False, description="https failed before any response; http was tried"
    )
    tls_verify: str = Field(default="", description="tls_verify of the first https hop")
    reflex: str = Field(
        default="",
        description="Reflex this response triggered in the worker, '' if none: "
        "'rate_limited' (429), 'retry_after', 'resets' (a burst of connect/headers "
        "failures on this IP), 'challenge' (obvious challenge page). The worker then "
        "stops the IP and releases its remaining tasks with a backoff",
    )
    elapsed_ms: float = Field(description="Wall time of the whole attempt, all hops")

    # Last hop's response, flattened for readers
    headers: list[tuple[str, str]] = Field(default_factory=list)
    cookie_names: list[str] = Field(
        default_factory=list, description="Names from Set-Cookie on every hop, in order"
    )
    content_type: str = ""
    content_encoding: str = ""
    content_length: int = Field(default=-1, description="Declared Content-Length; -1: none")

    # Body of the last hop
    body: str = Field(
        default="",
        description="Decoded text of the first body_bytes bytes. Charset as browsers pick "
        "it: byte order mark, Content-Type charset, <meta charset>; nothing declared: "
        "UTF-8 if valid, else windows-1252. Undecodable bytes are replaced (U+FFFD). "
        "Producers fill it with leasehold.decoding.decode_body; readers that want the "
        "text use leasehold.decoding.body_text",
    )
    body_bytes: int = Field(default=0, description="Bytes received, after content decoding")
    body_truncated: bool = Field(default=False, description="Cut off at the size limit")
    full_size: int = Field(
        default=-1,
        description="Decoded size of the whole body when known (a Content-Length without "
        "Content-Encoding, or the size of a body that wasn't cut); -1 unknown. With "
        "body_truncated, how much was left out",
    )
    body_charset: str = Field(default="", description="Python codec name used for body")
    body_decode_errors: int = Field(
        default=0,
        description="Characters replaced while decoding body (binary, or bytes invalid in "
        "the declared charset)",
    )
    body_sha256: str = Field(default="", description="Of the body_bytes bytes, hex")
    body_b64: str = Field(
        default="",
        description="The body_bytes bytes as received (after content decoding), base64: "
        "for readers that decode the body themselves",
    )
    body_text_dropped: bool = Field(
        default=False,
        description="body left empty because the event would exceed the size limit; "
        "body_b64 has the bytes",
    )
    body_ref_event: str = Field(
        default="",
        description="Event id of an earlier response with the same body bytes, status and "
        "final_url; body and body_b64 are then empty, body_bytes and body_sha256 describe "
        "the referenced body. Readers that judge the body (the classifier) skip such "
        "events and use the referenced event's result; '' when the body is in this event",
    )

    hops: list[Hop] = Field(default_factory=list)
