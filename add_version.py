"""Add a single AOP-Wiki quarter: download -> convert -> validate -> stats diff.

The one-command local equivalent of the quarterly-update workflow. It does not
touch git or versions.txt — it produces the artifacts and the stats diff; the
workflow (or the operator) decides what to commit.

Usage
-----
    # Detect the newest published quarter not yet in versions.txt (prints date)
    python add_version.py --detect

    # Process a specific quarter and write a Markdown stats diff
    python add_version.py 2026-07-01 --stats-out stats.md

Conversion reuses ``generate_all_rdf.process_version`` (same pipeline + validation
gate used for the full batch). The stats diff compares the freshly-converted
entity counts against the previous quarter pulled from the live SPARQL endpoint.

Exit codes: 0 success, 1 conversion/validation failure, 2 usage/download error.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import requests

import setup_versions

# NOTE: generate_all_rdf / aopwiki_rdf are imported lazily inside main(), only
# when a conversion is actually requested. This keeps --detect and --help working
# even when the heavy conversion package isn't installed in the environment.

logger = logging.getLogger(__name__)

ENDPOINT = os.getenv("SPARQL_ENDPOINT", "https://aopwiki-multirdf.vhp4safety.nl/sparql")
GRAPH_BASE = "http://aopwiki.org/graph/"

# Entity type URIs used both by the converter and by the AOP-Wiki RDF schema.
ENTITY_TYPES = {
    "AOPs": "http://aopkb.org/aop_ontology#AdverseOutcomePathway",
    "KEs": "http://aopkb.org/aop_ontology#KeyEvent",
    "KERs": "http://aopkb.org/aop_ontology#KeyEventRelationship",
    "Stressors": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C54571",
}


def _previous_version(new_date: str) -> str | None:
    """Latest known version strictly older than new_date (from versions.txt)."""
    older = sorted(v for v in setup_versions.load_versions() if v < new_date)
    return older[-1] if older else None


def _sparql_count(graph_date: str, type_uri: str, timeout: int = 60) -> int | None:
    """Count distinct subjects of a type in a version's named graph."""
    query = (
        f"SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE {{ "
        f"GRAPH <{GRAPH_BASE}{graph_date}> {{ ?s a <{type_uri}> }} }}"
    )
    try:
        resp = requests.get(
            ENDPOINT,
            params={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=timeout,
        )
        resp.raise_for_status()
        bindings = resp.json()["results"]["bindings"]
        return int(bindings[0]["n"]["value"]) if bindings else 0
    except (requests.RequestException, KeyError, ValueError) as exc:
        logger.warning("Could not fetch %s count for %s: %s", type_uri, graph_date, exc)
        return None


def build_stats_diff(new_date: str, result) -> str:
    """Build a Markdown stats table comparing the new quarter to the previous one."""
    prev = _previous_version(new_date)
    new_counts = {
        "AOPs": result.aop_count,
        "KEs": result.ke_count,
        "KERs": result.ker_count,
        "Stressors": result.stressor_count,
    }
    lines = [f"## New AOP-Wiki RDF version: {new_date}", ""]
    if prev:
        lines.append(f"Entity counts vs previous quarter (`{prev}`, from the live endpoint):")
        lines.append("")
        lines.append("| Entity | New | Previous | Δ |")
        lines.append("|---|--:|--:|--:|")
        for label in ("AOPs", "KEs", "KERs", "Stressors"):
            new_n = new_counts[label]
            prev_n = _sparql_count(prev, ENTITY_TYPES[label])
            if prev_n is None:
                delta = "n/a"
                prev_disp = "n/a"
            else:
                d = new_n - prev_n
                delta = f"{d:+d}"
                prev_disp = str(prev_n)
            lines.append(f"| {label} | {new_n} | {prev_disp} | {delta} |")
    else:
        lines.append("No previous version found for comparison. New counts:")
        lines.append("")
        lines.append("| Entity | Count |")
        lines.append("|---|--:|")
        for label in ("AOPs", "KEs", "KERs", "Stressors"):
            lines.append(f"| {label} | {new_counts[label]} |")
    lines.append("")
    lines.append(f"Total triples (4 TTL files): **{result.triple_count}**")
    return "\n".join(lines) + "\n"


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("date", nargs="?", help="Quarter date YYYY-MM-DD (omit with --detect).")
    p.add_argument("--detect", action="store_true",
                   help="Print the newest published quarter not yet in versions.txt, then exit.")
    p.add_argument("--stats-out", metavar="FILE", help="Write the Markdown stats diff to FILE.")
    p.add_argument("--bridgedb-url", default=None, help="BridgeDb service URL.")
    p.add_argument("--force", action="store_true", help="Reprocess even if TTL files exist.")
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def main() -> int:
    args = _build_parser().parse_args()
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()],
    )
    logging.getLogger("aopwiki_rdf").setLevel(logging.INFO if args.verbose else logging.WARNING)

    if args.detect:
        nxt = setup_versions.next_missing_quarter()
        if nxt:
            print(nxt)
        return 0

    date = args.date
    if not date:
        date = setup_versions.next_missing_quarter()
        if not date:
            logger.info("No new quarter to add.")
            return 0
        logger.info("Auto-detected new quarter: %s", date)

    # Heavy imports deferred until we actually convert.
    from generate_all_rdf import _check_bridgedb, _ensure_prefixes_symlink, process_version
    from aopwiki_rdf.config import PipelineConfig

    # Download
    gz_path = setup_versions.download_version(date)
    if not gz_path or not Path(gz_path).exists():
        logger.error("Download failed for %s", date)
        return 2

    # Convert + validate (reuse the batch pipeline for one version)
    _ensure_prefixes_symlink()
    bridgedb_url = args.bridgedb_url or PipelineConfig.bridgedb_url
    logger.info("BridgeDb pre-flight check at %s ...", bridgedb_url)
    if not _check_bridgedb(bridgedb_url):
        logger.error("BridgeDb unreachable at %s", bridgedb_url)
        return 2

    config = PipelineConfig(
        data_dir=Path(gz_path).parent,
        bridgedb_url=bridgedb_url,
        request_timeout=30,
        log_level="WARNING",
    )
    result = process_version(gz_path, date, config, args.force)
    logger.info("%s -> %s (%.1fs)", date, result.status, result.duration)

    if result.status not in ("PASS", "SKIP"):
        logger.error("Conversion/validation failed for %s: %s", date, result.error)
        return 1

    stats = build_stats_diff(date, result)
    print("\n" + stats)
    if args.stats_out:
        Path(args.stats_out).write_text(stats)
        logger.info("Stats diff written to %s", args.stats_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
