import json
from pathlib import Path

from leasehold.cli import main

FIXTURES = Path(__file__).parent / "fixtures"


def test_validate_directory(capsys):
    assert main(["validate", str(FIXTURES)]) == 0
    out = capsys.readouterr().out
    assert "ok" in out and "certwatch" in out and "resolver" in out


def test_validate_single_service_needs_partial(capsys):
    resolver = str(FIXTURES / "resolver" / "service.yml")
    assert main(["validate", resolver]) == 1
    assert "no service produces" in capsys.readouterr().out
    assert main(["validate", "--partial", resolver]) == 0


def test_validate_reports_bad_file(tmp_path, capsys):
    bad = tmp_path / "service.yml"
    bad.write_text("spec_version: 1\nname: Bad_Name\n")
    assert main(["validate", str(bad)]) == 1
    assert "FAIL" in capsys.readouterr().out


def test_names(capsys):
    assert main(["names", str(FIXTURES / "certwatch" / "service.yml"), "--env", "dev"]) == 0
    assert "dev.certwatch.entries.v1" in capsys.readouterr().out


def test_schema_is_json(capsys):
    assert main(["schema"]) == 0
    schema = json.loads(capsys.readouterr().out)
    assert schema["title"] == "leasehold service.yml"
    assert "components" in schema["properties"]
