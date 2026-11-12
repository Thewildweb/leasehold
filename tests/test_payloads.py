"""Payload validation: against the topic's model when producing and when consuming."""

from __future__ import annotations

import copy
import dataclasses
import json

import pytest
from conftest import RUNTIME_SPEC, FakeControl, FakeKafkaProducer, runtime_cfg, runtime_spec
from payload_models import S
from test_runtime_parts import FakeConsumer, Msg, _produced

from leasehold import nomad
from leasehold.runtime import payloads
from leasehold.runtime.context import Context
from leasehold.runtime.events import EventProducer, InvalidPayload
from leasehold.runtime.processor import INVALID, Processor
from leasehold.spec import ServiceSpec


def _producer(component="worker"):
    kafka = FakeKafkaProducer()
    return EventProducer(runtime_cfg(component), producer=kafka), kafka


def _ctx(cfg):
    return Context(cfg, client=FakeControl(), producer=_producer(cfg.component)[0])


def test_load_model_errors():
    assert payloads.load_model("payload_models:R").__name__ == "R"
    for path, msg in [
        ("no_colon", "module:Class"),
        ("nowhere_xyz:M", "can't be imported"),
        ("payload_models:Missing", "can't be imported"),
        ("json:dumps", "not a pydantic model"),
    ]:
        with pytest.raises(payloads.PayloadModelError, match=msg):
            payloads.load_model(path)


def test_producer_refuses_payload_that_is_not_the_model():
    p, kafka = _producer()
    with pytest.raises(InvalidPayload, match="results: id: Field required"):
        p.emit("results", {"n": 1})
    with pytest.raises(InvalidPayload, match="id: Input should be a valid string"):
        p.emit("results", {"id": ["a"]})
    assert kafka.messages == []


def test_producer_sends_the_models_json():
    p, kafka = _producer()
    p.emit("results", {"id": "a", "extra": 1})
    ((_, key, value),) = kafka.messages
    assert key == b"a"
    assert json.loads(value)["payload"] == {"id": "a", "extra": 1}
    p, kafka = _producer("router")
    p.emit("routed", S())  # a model instance; the default is filled in
    assert json.loads(kafka.messages[0][2])["payload"] == {"n": 0}


def test_producer_fails_at_start_when_a_model_is_missing():
    data = copy.deepcopy(RUNTIME_SPEC)
    data["kafka"]["produces"]["results"]["payload_model"] = "nowhere_xyz:R"
    cfg = dataclasses.replace(runtime_cfg("worker"), spec=ServiceSpec.model_validate(data))
    with pytest.raises(payloads.PayloadModelError, match="nowhere_xyz"):
        EventProducer(cfg, producer=FakeKafkaProducer())


def test_processor_skips_and_counts_events_whose_payload_is_invalid():
    seen = []
    (topic, key, good), *_ = _produced(1)
    env = json.loads(good)
    env["payload"] = {"no_id": True}
    bad = Msg(topic, json.dumps(env).encode(), offset=4, key=key)
    ok = Msg(topic, good, offset=5, key=key)
    cfg = runtime_cfg("router")
    ctx = Context(
        cfg, client=FakeControl(), producer=EventProducer(cfg, producer=FakeKafkaProducer())
    )
    consumer = FakeConsumer([[bad, ok]])
    p = Processor(ctx, lambda ev, c: seen.extend(ev), consumer=consumer)
    before = INVALID.labels(topic)._value.get()
    assert p.step()
    assert [e.offset for e in seen] == [5]
    assert INVALID.labels(topic)._value.get() == before + 1
    assert consumer.commits == [[(topic, 0, 6)]]  # past the invalid one too


def test_processor_needs_models_for_what_it_consumes():
    cfg = runtime_cfg("router", payload_models={})
    ctx = Context(
        cfg, client=FakeControl(), producer=EventProducer(cfg, producer=FakeKafkaProducer())
    )
    with pytest.raises(payloads.PayloadModelError, match="jobs.results.v1"):
        Processor(ctx, lambda ev, c: None, consumer=FakeConsumer([]))


def test_nomad_passes_consumed_models_to_processors():
    platform = nomad.Platform(registry="r.example", api_url="http://api", kafka_bootstrap="k:9092")
    p = nomad.plan([runtime_spec()], "dev", "abc1234", platform)
    envs = {j.component: j.job["TaskGroups"][0]["Tasks"][0]["Env"] for j in p.jobs}
    assert json.loads(envs["router"]["LEASEHOLD_PAYLOAD_MODELS"]) == {
        "jobs.results.v1": "payload_models:R"
    }
    assert "LEASEHOLD_PAYLOAD_MODELS" not in envs["worker"]
