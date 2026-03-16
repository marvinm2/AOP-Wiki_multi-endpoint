"""Batch compatibility test for all 30 AOP-Wiki XML versions.

Standalone script — no test framework required. Run with:
    python test_compatibility.py                         # all versions
    python test_compatibility.py --version 2018-04-01   # single version
"""
import argparse
import gzip
import logging
import os
import re
import sys
import tempfile
from pathlib import Path

from aopwiki_rdf.parser.xml_parser import parse_aopwiki_xml

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
VERSIONS_DIR = Path(__file__).parent / "versions"
COMPATIBILITY_MD = Path(__file__).parent / "COMPATIBILITY.md"

logging.basicConfig(
    format="%(levelname)s %(name)s: %(message)s",
    level=logging.WARNING,
)
logger = logging.getLogger("test_compatibility")

# ---------------------------------------------------------------------------
# Baseline entity counts from direct XML inspection (vendor-specific refs)
# Source: 02-RESEARCH.md Full Baseline Counts table
# ---------------------------------------------------------------------------
BASELINE = {
    "2018-04-01": {"AOPs": 219, "KEs": 911,  "KERs": 1056, "Stressors": 306},
    "2018-07-01": {"AOPs": 227, "KEs": 936,  "KERs": 1085, "Stressors": 332},
    "2018-10-01": {"AOPs": 230, "KEs": 930,  "KERs": 1077, "Stressors": 332},
    "2019-01-01": {"AOPs": 236, "KEs": 961,  "KERs": 1115, "Stressors": 345},
    "2019-04-01": {"AOPs": 246, "KEs": 980,  "KERs": 1144, "Stressors": 380},
    "2019-07-01": {"AOPs": 261, "KEs": 1022, "KERs": 1199, "Stressors": 430},
    "2019-10-01": {"AOPs": 267, "KEs": 1043, "KERs": 1220, "Stressors": 434},
    "2020-01-01": {"AOPs": 273, "KEs": 1060, "KERs": 1239, "Stressors": 444},
    "2020-04-01": {"AOPs": 280, "KEs": 1080, "KERs": 1259, "Stressors": 454},
    "2020-07-01": {"AOPs": 304, "KEs": 1111, "KERs": 1318, "Stressors": 477},
    "2020-10-01": {"AOPs": 306, "KEs": 1118, "KERs": 1338, "Stressors": 499},
    "2021-01-03": {"AOPs": 316, "KEs": 1131, "KERs": 1363, "Stressors": 523},
    "2021-04-01": {"AOPs": 333, "KEs": 1149, "KERs": 1382, "Stressors": 534},
    "2021-07-01": {"AOPs": 354, "KEs": 1184, "KERs": 1465, "Stressors": 552},
    "2021-10-01": {"AOPs": 371, "KEs": 1201, "KERs": 1493, "Stressors": 573},
    "2022-01-01": {"AOPs": 379, "KEs": 1237, "KERs": 1529, "Stressors": 598},
    "2022-04-01": {"AOPs": 390, "KEs": 1252, "KERs": 1579, "Stressors": 619},
    "2022-07-01": {"AOPs": 405, "KEs": 1273, "KERs": 1636, "Stressors": 624},
    "2022-10-01": {"AOPs": 416, "KEs": 1323, "KERs": 1715, "Stressors": 635},
    "2023-01-01": {"AOPs": 424, "KEs": 1335, "KERs": 1743, "Stressors": 641},
    "2023-04-01": {"AOPs": 437, "KEs": 1355, "KERs": 1758, "Stressors": 645},
    "2023-07-01": {"AOPs": 448, "KEs": 1387, "KERs": 1841, "Stressors": 650},
    "2023-10-01": {"AOPs": 458, "KEs": 1416, "KERs": 1906, "Stressors": 652},
    "2024-01-01": {"AOPs": 465, "KEs": 1425, "KERs": 1951, "Stressors": 652},
    "2024-04-01": {"AOPs": 474, "KEs": 1434, "KERs": 1974, "Stressors": 652},
    "2024-07-01": {"AOPs": 482, "KEs": 1443, "KERs": 2014, "Stressors": 655},
    "2024-10-01": {"AOPs": 487, "KEs": 1456, "KERs": 2041, "Stressors": 658},
    "2025-01-01": {"AOPs": 509, "KEs": 1491, "KERs": 2106, "Stressors": 717},
    "2025-04-01": {"AOPs": 519, "KEs": 1502, "KERs": 2129, "Stressors": 721},
    "2025-07-01": {"AOPs": 525, "KEs": 1497, "KERs": 2132, "Stressors": 720},
}

VERSION_DATE_RE = re.compile(r"aop-wiki-xml-(\d{4}-\d{2}-\d{2})\.gz$")


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def find_gz_files(base_dir: Path) -> list:
    """Walk base_dir and return sorted list of aop-wiki-xml-*.gz paths."""
    found = []
    for root, _, names in os.walk(base_dir):
        for name in names:
            if name.startswith("aop-wiki-xml-") and name.endswith(".gz"):
                found.append(Path(root) / name)
    return sorted(found)


def extract_version_date(gz_path: Path) -> str:
    """Extract YYYY-MM-DD from an aop-wiki-xml-YYYY-MM-DD.gz filename."""
    m = VERSION_DATE_RE.search(gz_path.name)
    if m:
        return m.group(1)
    raise ValueError(f"Cannot extract version date from: {gz_path.name}")


def test_version(gz_path: Path, version_date: str) -> dict:
    """Decompress gz_path to a temp XML file and run parse_aopwiki_xml().

    Returns a dict with keys:
        status  - "PASS" or "FAIL"
        counts  - dict of AOPs/KEs/KERs/Stressors (None on FAIL)
        error   - error string (None on PASS)
        notes   - optional free-text notes
    """
    with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        with gzip.open(str(gz_path), "rb") as f_in:
            with open(tmp_path, "wb") as f_out:
                f_out.write(f_in.read())

        entities = parse_aopwiki_xml(tmp_path, config=None, version_date=version_date)
        counts = {
            "AOPs":      len(entities.aopdict),
            "KEs":       len(entities.kedict),
            "KERs":      len(entities.kerdict),
            "Stressors": len(entities.stressordict),
        }
        return {"status": "PASS", "counts": counts, "error": None, "notes": ""}

    except Exception as exc:
        return {
            "status": "FAIL",
            "counts": None,
            "error": f"{type(exc).__name__}: {exc}",
            "notes": "",
        }
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# COMPATIBILITY.md generation
# ---------------------------------------------------------------------------

def _delta_note(actual: int, expected: int, key: str) -> str:
    """Return a brief note if actual differs from expected baseline."""
    if expected is None or actual == expected:
        return ""
    diff = actual - expected
    sign = "+" if diff > 0 else ""
    return f"[{key} {sign}{diff} vs baseline {expected}]"


def write_compatibility_matrix(results: dict, baseline: dict, output_path: Path):
    """Generate a Markdown compatibility matrix and write it to output_path."""
    total = len(results)
    passes = sum(1 for r in results.values() if r["status"] == "PASS")
    fails = total - passes

    lines = [
        "# AOP-Wiki XML Compatibility Matrix",
        "",
        "Auto-generated by `test_compatibility.py`. Re-run after parser patches.",
        "",
        f"**Summary:** {passes}/{total} versions PASS" + (f", {fails} FAIL" if fails else "") + ".",
        "",
        "| Version | Status | AOPs | KEs | KERs | Stressors | Notes |",
        "|---------|:------:|-----:|----:|-----:|----------:|-------|",
    ]

    for version in sorted(results):
        result = results[version]
        b = baseline.get(version, {})

        if result["status"] == "PASS":
            c = result["counts"]
            notes_parts = []
            for key in ("AOPs", "KEs", "KERs", "Stressors"):
                note = _delta_note(c[key], b.get(key), key)
                if note:
                    notes_parts.append(note)
            notes = " ".join(notes_parts) if notes_parts else "matches baseline"
            row = (
                f"| {version} | PASS | {c['AOPs']} | {c['KEs']} | {c['KERs']} "
                f"| {c['Stressors']} | {notes} |"
            )
        else:
            row = f"| {version} | **FAIL** | — | — | — | — | {result['error']} |"

        lines.append(row)

    lines += [
        "",
        "## Notes",
        "",
        "- Baseline counts come from `vendor-specific` reference tables in the XML.",
        "- Entity count differences from baseline are informational only — early versions had fewer entities.",
        "- A failed status means `parse_aopwiki_xml()` raised an exception (schema issue).",
        "- Schema transition: 2022-04-01 and earlier use `id` attribute; 2022-07-01+ use `key-event-id`.",
    ]

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.warning(f"Written {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Test AOP-Wiki XML parser compatibility across all historical versions."
    )
    parser.add_argument(
        "--version",
        metavar="YYYY-MM-DD",
        help="Test a single version by date instead of all versions.",
    )
    args = parser.parse_args()

    if not VERSIONS_DIR.exists():
        print(f"ERROR: versions directory not found: {VERSIONS_DIR}", file=sys.stderr)
        sys.exit(2)

    # Find gz files
    all_gz = find_gz_files(VERSIONS_DIR)
    if not all_gz:
        print("ERROR: No aop-wiki-xml-*.gz files found under versions/", file=sys.stderr)
        sys.exit(2)

    # Filter to a single version if requested
    if args.version:
        filtered = [p for p in all_gz if args.version in p.name]
        if not filtered:
            print(f"ERROR: No .gz file found for version {args.version!r}", file=sys.stderr)
            sys.exit(2)
        gz_files = filtered
    else:
        gz_files = all_gz

    results = {}
    pass_count = 0
    fail_count = 0

    for gz_path in gz_files:
        version_date = extract_version_date(gz_path)
        result = test_version(gz_path, version_date)
        results[version_date] = result

        status_sym = "PASS" if result["status"] == "PASS" else "FAIL"
        if result["status"] == "PASS":
            c = result["counts"]
            print(f"  {status_sym}  {version_date}  AOPs={c['AOPs']} KEs={c['KEs']} KERs={c['KERs']} Stressors={c['Stressors']}")
            pass_count += 1
        else:
            print(f"  {status_sym}  {version_date}  {result['error']}")
            fail_count += 1

    # Generate COMPATIBILITY.md when running all versions (or even a single one)
    write_compatibility_matrix(results, BASELINE, COMPATIBILITY_MD)

    print()
    print(f"Results: {pass_count} PASS, {fail_count} FAIL out of {len(results)} versions.")
    print(f"Compatibility matrix written to {COMPATIBILITY_MD}")

    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
