"""scripts/add-quarter.sh (#22): the argument guards, offline (nothing is fetched or loaded)."""

import pathlib
import subprocess

SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "scripts" / "add-quarter.sh"


def run(*args):
    return subprocess.run(["bash", str(SCRIPT), *args], capture_output=True, text=True, timeout=30)


def test_help_lists_the_options():
    out = run("--help")
    assert out.returncode == 0
    for option in ("--run", "--from", "--graph", "--clear-after", "--replace", "--dry-run", "--yes"):
        assert option in out.stdout


def test_the_date_must_be_a_quarter_date():
    out = run("2026-10")
    assert out.returncode != 0 and "DATE must be YYYY-MM-DD" in out.stderr


def test_a_test_graph_may_not_sit_under_the_live_prefix():
    out = run("2026-10-01", "--graph", "http://aopwiki.org/graph/2026-10-01-test", "--dry-run")
    assert out.returncode != 0 and "must be http://aopwiki.org/graph/2026-10-01" in out.stderr


def test_missing_files_are_refused_before_anything_is_uploaded(tmp_path):
    (tmp_path / "AOPWikiRDF-2026-10-01.ttl").write_text("@prefix ex: <http://example.org/> .\n")
    out = run("2026-10-01", "--from", str(tmp_path), "--no-catalogue", "--dry-run")
    assert out.returncode != 0 and "missing" in out.stderr
    assert "Uploading" not in out.stdout
