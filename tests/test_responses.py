"""The response event model: defaults, and subclasses that add their own fields."""

from __future__ import annotations

from leasehold.responses import Hop, Response


def test_minimal_event_and_subclass():
    class ServiceResponse(Response):
        tier: str = ""

    r = ServiceResponse(
        domain="example.nl",
        url="https://example.nl/",
        target_ip="192.0.2.1",
        attempt=1,
        client="test",
        impersonate="",
        user_agent="ua",
        elapsed_ms=1.0,
        tier="dutch",
        hops=[Hop(url="https://example.nl/", verify=True, status=200)],
    )
    d = r.model_dump()
    assert d["schema_version"] == 1 and d["failure_stage"] == "none"
    assert d["tier"] == "dutch"
    # A reader that knows only the base model ignores the service's own fields.
    base = Response.model_validate(d)
    assert base.hops[0].status == 200 and not hasattr(base, "tier")


def test_body_ref_event_survives_the_base_model():
    # A producer that skips an unchanged body names the event holding it; the classifier
    # reads with the base model and must still see the reference to skip the event.
    d = Response(
        domain="example.nl",
        url="https://example.nl/a",
        target_ip="192.0.2.1",
        attempt=1,
        client="test",
        impersonate="",
        user_agent="ua",
        elapsed_ms=1.0,
        status=200,
        body_bytes=1234,
        body_sha256="ab" * 32,
        body_ref_event="evt-1",
    ).model_dump()
    base = Response.model_validate(d)
    assert base.body_ref_event == "evt-1" and base.body == ""
    assert Response.model_validate({**d, "body_ref_event": ""}).body_ref_event == ""
