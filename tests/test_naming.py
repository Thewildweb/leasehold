import pytest

from leasehold import naming
from leasehold.naming import NamingError, TopicRef


def test_prod_has_no_prefix_and_dev_does():
    assert naming.topic("prod", "certlog", "entries", 1) == "certlog.entries.v1"
    assert naming.topic("dev", "certlog", "entries", 1) == "dev.certlog.entries.v1"
    assert naming.clickhouse_database("prod", "ct_logs") == "ct_logs"
    assert naming.clickhouse_database("dev", "ct_logs") == "dev_ct_logs"
    assert naming.postgres_schema("dev", "frontier") == "dev_frontier"
    assert naming.nomad_namespace("prod") == "default"
    assert naming.nomad_namespace("dev") == "dev"


def test_nomad_jobs_are_not_prefixed():
    assert naming.nomad_job("certlog", "worker") == "certlog-worker"


def test_consumer_groups_differ_per_env():
    assert naming.consumer_group("prod", "dns", "new-domains") == "dns.new-domains"
    assert naming.consumer_group("dev", "dns", "new-domains") == "dev.dns.new-domains"


def test_topic_ref_roundtrip():
    ref = TopicRef.parse("certlog.entries.v2")
    assert ref == TopicRef("certlog", "entries", 2)
    assert str(ref) == "certlog.entries.v2"
    assert naming.topic_for("dev", ref) == "dev.certlog.entries.v2"


@pytest.mark.parametrize(
    "bad", ["certlog.entries", "certlog.entries.v0", "Cert.entries.v1", "a.b_c.v1"]
)
def test_topic_ref_rejects_malformed(bad):
    with pytest.raises(NamingError):
        TopicRef.parse(bad)


@pytest.mark.parametrize("bad", ["", "Dev", "1dev", "dev-1", "averyveryverylongenv"])
def test_invalid_env(bad):
    with pytest.raises(NamingError):
        naming.check_env(bad)


def test_topic_names_reject_underscores():
    # Kafka metric names collide when '.' and '_' are mixed in topic names.
    with pytest.raises(NamingError):
        naming.topic("prod", "certlog", "chunk_entries", 1)


def test_image_ref_requires_sha():
    assert (
        naming.image_ref("ghcr.io/org/", "certlog", "worker", "a1b2c3d")
        == "ghcr.io/org/certlog-worker:a1b2c3d"
    )
    with pytest.raises(NamingError):
        naming.image_ref("ghcr.io/org", "certlog", "worker", "latest")
