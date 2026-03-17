"""Batch orchestrator: convert all AOP-Wiki XML versions to RDF.

Calls the aopwiki_rdf package API directly (no child processes).
Processes all local .gz files in the versions/ directory.
"""

import argparse
import dataclasses
import gzip
import logging
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional
from xml.etree.ElementTree import parse as et_parse

import requests

from validate_rdf import validate_version_dir
from aopwiki_rdf.config import PipelineConfig
from aopwiki_rdf.parser.xml_parser import parse_aopwiki_xml, AOPXML_NS
from aopwiki_rdf.pipeline import (
    _stage_setup,
    _stage_chemicals,
    _stage_protein_ontology,
    _stage_gene_mapping,
    _stage_write_aop_rdf,
    _stage_write_enriched_rdf,
    _stage_write_genes_rdf,
    _stage_write_void_rdf,
)

logger = logging.getLogger(__name__)

VERSIONS_DIR = "versions"

# Maps stage-default output filenames to the versioned naming convention.
_DEFAULT_TO_VERSIONED_TEMPLATE = [
    ("AOPWikiRDF.ttl",          "AOPWikiRDF-{version}.ttl"),
    ("AOPWikiRDF-Enriched.ttl", "AOPWikiRDF-Enriched-{version}.ttl"),
    ("AOPWikiRDF-Genes.ttl",    "AOPWikiRDF-Genes-{version}.ttl"),
    ("AOPWikiRDF-Void.ttl",     "AOPWikiRDF-Void-{version}.ttl"),
]


@dataclasses.dataclass
class VersionResult:
    version: str
    status: str          # "PASS" | "FAIL" | "SKIP" | "INVALID"
    duration: float      # seconds
    aop_count: int = 0
    ke_count: int = 0
    ker_count: int = 0
    stressor_count: int = 0
    triple_count: int = 0   # sum of triples across all 4 TTL files
    error: Optional[str] = None


def find_all_gz_files(base_dir: str) -> list:
    """Walk base_dir and return sorted list of aop-wiki-xml-*.gz paths."""
    gz_files = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".gz") and file.startswith("aop-wiki-xml-"):
                gz_files.append(os.path.join(root, file))
    return sorted(gz_files)


def _parse_local_gz(gz_path: str, version_date: str, config, context: dict) -> None:
    """Decompress .gz and parse XML; replaces _stage_parse for local files.

    Sets context keys: entities, xml_root, aopxml_ns, aopwikixmlfilename.
    Both parse_aopwiki_xml and et_parse need a real file path, so the .gz
    is decompressed to a NamedTemporaryFile first.
    """
    with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as tmp:
        tmp_xml_path = tmp.name
        with gzip.open(gz_path, "rb") as f_in:
            shutil.copyfileobj(f_in, tmp)

    try:
        entities = parse_aopwiki_xml(tmp_xml_path, config=None, version_date=version_date)
        tree = et_parse(tmp_xml_path)
        xml_root = tree.getroot()
    finally:
        os.unlink(tmp_xml_path)

    context["entities"] = entities
    context["xml_root"] = xml_root
    context["aopxml_ns"] = AOPXML_NS
    # aopwikixmlfilename is used by _stage_write_void_rdf for provenance metadata.
    context["aopwikixmlfilename"] = f"aop-wiki-xml-{version_date}.gz"


def _check_bridgedb(bridgedb_url: str, timeout: int = 10) -> bool:
    """Return True if BridgeDb endpoint is reachable, False otherwise.

    Probes the /properties sub-endpoint (used by _stage_write_void_rdf) rather
    than the bare base URL, which returns 404 on the public BridgeDb service.
    """
    probe_url = bridgedb_url.rstrip("/") + "/properties"
    try:
        resp = requests.get(probe_url, timeout=timeout)
        resp.raise_for_status()
        return True
    except requests.RequestException:
        return False


def _atomic_rename_outputs(version_dir: Path, version: str) -> None:
    """Rename stage default output filenames to versioned final names.

    Uses os.replace() which is POSIX-atomic (rename(2)) on Linux when both
    paths share the same filesystem.  The unversioned defaults act as
    in-progress artifacts; this call promotes them to permanent names.
    """
    for src_name, dst_template in _DEFAULT_TO_VERSIONED_TEMPLATE:
        src = version_dir / src_name
        dst = version_dir / dst_template.format(version=version)
        if src.exists():
            os.replace(src, dst)


def _cleanup_default_outputs(version_dir: Path) -> None:
    """Delete any unversioned stage output files left by a failed run."""
    for src_name, _ in _DEFAULT_TO_VERSIONED_TEMPLATE:
        path = version_dir / src_name
        if path.exists():
            path.unlink()


def process_version(gz_path: str, version: str, config: PipelineConfig, force: bool) -> VersionResult:
    """Process a single version end-to-end.

    Parameters
    ----------
    gz_path:
        Full path to the .gz input file.
    version:
        Date string, e.g. "2025-07-01".
    config:
        PipelineConfig with data_dir set to this version's directory.
        Must be created per-version (not shared across versions).
    force:
        If False and all 4 versioned TTL files already exist, return SKIP.
    """
    version_dir = Path(gz_path).parent
    t0 = time.time()

    # Skip check: all 4 versioned TTL files must exist to qualify for skip.
    expected = [
        version_dir / f"AOPWikiRDF-{version}.ttl",
        version_dir / f"AOPWikiRDF-Enriched-{version}.ttl",
        version_dir / f"AOPWikiRDF-Genes-{version}.ttl",
        version_dir / f"AOPWikiRDF-Void-{version}.ttl",
    ]
    if not force and all(p.exists() for p in expected):
        return VersionResult(version=version, status="SKIP", duration=time.time() - t0)

    try:
        context: dict = {}
        _stage_setup(config, context)
        _parse_local_gz(gz_path, version, config, context)
        _stage_chemicals(config, context)
        _stage_protein_ontology(config, context)
        _stage_gene_mapping(config, context)
        _stage_write_aop_rdf(config, context)
        _stage_write_enriched_rdf(config, context)
        _stage_write_genes_rdf(config, context)
        _stage_write_void_rdf(config, context)

        # --- Validation gate (PIPE-05, PIPE-06) ---
        entities = context["entities"]
        val = validate_version_dir(
            version_dir=version_dir,
            aop_count=len(entities.aopdict),
            ke_count=len(entities.kedict),
            ker_count=len(entities.kerdict),
            stressor_count=len(entities.stressordict),
        )
        if not val.valid:
            _cleanup_default_outputs(version_dir)
            errors = "; ".join(
                [fr.error for fr in val.file_results if fr.error]
                + ([val.entity_error] if val.entity_error else [])
            )
            return VersionResult(
                version=version,
                status="INVALID",
                duration=time.time() - t0,
                aop_count=len(entities.aopdict),
                ke_count=len(entities.kedict),
                ker_count=len(entities.kerdict),
                stressor_count=len(entities.stressordict),
                triple_count=val.total_triples,
                error=errors,
            )

        _atomic_rename_outputs(version_dir, version)
        return VersionResult(
            version=version,
            status="PASS",
            duration=time.time() - t0,
            aop_count=len(entities.aopdict),
            ke_count=len(entities.kedict),
            ker_count=len(entities.kerdict),
            stressor_count=len(entities.stressordict),
            triple_count=val.total_triples,
        )
    except Exception as exc:
        _cleanup_default_outputs(version_dir)
        return VersionResult(
            version=version,
            status="FAIL",
            duration=time.time() - t0,
            error=str(exc),
        )


def _print_summary(results: list) -> None:
    """Log summary table of all version results."""
    passed  = sum(1 for r in results if r.status == "PASS")
    failed  = sum(1 for r in results if r.status == "FAIL")
    skipped = sum(1 for r in results if r.status == "SKIP")
    invalid = sum(1 for r in results if r.status == "INVALID")
    total_secs = sum(r.duration for r in results)

    logger.info("")
    logger.info("%-14s %-8s %-10s %5s %5s %5s %9s %9s",
                "Version", "Status", "Duration", "AOPs", "KEs", "KERs", "Stressors", "Triples")
    logger.info("-" * 72)
    for r in results:
        if r.status == "SKIP":
            logger.info("%-14s %-8s %-10s %5s %5s %5s %9s %9s",
                        r.version, r.status, f"{r.duration:.1f}s", "-", "-", "-", "-", "-")
        elif r.status == "FAIL":
            logger.info("%-14s %-8s %-10s  ERROR: %s",
                        r.version, r.status, f"{r.duration:.1f}s", r.error or "unknown")
        elif r.status == "INVALID":
            logger.info("%-14s %-8s %-10s %5d %5d %5d %9d %9d  INVALID: %s",
                        r.version, r.status, f"{r.duration:.1f}s",
                        r.aop_count, r.ke_count, r.ker_count, r.stressor_count,
                        r.triple_count, r.error or "unknown")
        else:
            logger.info("%-14s %-8s %-10s %5d %5d %5d %9d %9d",
                        r.version, r.status, f"{r.duration:.1f}s",
                        r.aop_count, r.ke_count, r.ker_count, r.stressor_count,
                        r.triple_count)

    logger.info("")
    h, remainder = divmod(int(total_secs), 3600)
    m, s = divmod(remainder, 60)
    logger.info(
        "%d/%d passed, %d failed, %d invalid, %d skipped | Total: %dh%02dm%02ds",
        passed, len(results), failed, invalid, skipped, h, m, s,
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Convert all AOP-Wiki XML versions to RDF using the aopwiki_rdf package API. "
            "Reads local .gz files from the versions/ directory."
        )
    )
    p.add_argument(
        "--bridgedb-url",
        default=None,
        help=(
            "BridgeDb service URL "
            "(default: %(default)s — falls back to PipelineConfig default)"
        ),
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Reprocess versions even if all 4 TTL files already exist.",
    )
    p.add_argument(
        "--version",
        dest="versions",
        action="append",
        metavar="DATE",
        help=(
            "Process only this version date (repeatable). "
            "Example: --version 2024-01-01 --version 2025-07-01"
        ),
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Set the aopwiki_rdf package logger to INFO (default: WARNING).",
    )
    return p


def _ensure_prefixes_symlink() -> None:
    """Ensure prefixes.csv is resolvable from the current working directory.

    The upstream _stage_write_aop_rdf hardcodes the relative path "prefixes.csv"
    (pipeline.py line 282).  In this project the actual file is at data/prefixes.csv.
    This function creates a symlink `prefixes.csv -> data/prefixes.csv` in CWD if
    neither a symlink nor a file named prefixes.csv already exists there.
    """
    target = Path("prefixes.csv")
    source = Path("data/prefixes.csv")
    if target.exists() or target.is_symlink():
        return
    if not source.exists():
        logger.warning(
            "data/prefixes.csv not found — _stage_write_aop_rdf may fail. "
            "Run from the Setup directory."
        )
        return
    try:
        target.symlink_to(source)
        logger.info("Created symlink prefixes.csv -> data/prefixes.csv")
    except OSError as exc:
        logger.warning("Could not create prefixes.csv symlink: %s", exc)


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()],
    )
    pkg_level = logging.INFO if verbose else logging.WARNING
    logging.getLogger("aopwiki_rdf").setLevel(pkg_level)


def main() -> None:
    args = _build_parser().parse_args()
    _setup_logging(args.verbose)

    # Resolve BridgeDb URL: CLI flag > PipelineConfig default.
    bridgedb_url = args.bridgedb_url or PipelineConfig.bridgedb_url

    # Ensure prefixes.csv is accessible from CWD.
    # The upstream _stage_write_aop_rdf hardcodes "prefixes.csv" as a CWD-relative
    # path.  The actual file lives at data/prefixes.csv.  Create a symlink so
    # both paths work when the script is run from the Setup directory.
    _ensure_prefixes_symlink()

    # Discover all available versions.
    all_gz = find_all_gz_files(VERSIONS_DIR)
    if not all_gz:
        logger.error("No .gz files found under %s/", VERSIONS_DIR)
        sys.exit(1)

    # Build version -> gz_path mapping.
    version_map: dict = {}
    for gz_path in all_gz:
        filename = os.path.basename(gz_path)
        version = filename.replace("aop-wiki-xml-", "").replace(".gz", "")
        version_map[version] = gz_path

    # Apply --version filter if provided.
    if args.versions:
        missing = [v for v in args.versions if v not in version_map]
        if missing:
            logger.error("Requested versions not found in %s/: %s", VERSIONS_DIR, missing)
            sys.exit(1)
        versions_to_process = [(v, version_map[v]) for v in args.versions]
    else:
        versions_to_process = sorted(version_map.items())

    total = len(versions_to_process)
    logger.info("Found %d version(s) to consider.", total)

    # BridgeDb pre-flight check — fail fast before starting the batch.
    logger.info("BridgeDb pre-flight check... connecting to %s", bridgedb_url)
    if not _check_bridgedb(bridgedb_url):
        logger.error(
            "BridgeDb pre-flight check... ERROR: BridgeDb unreachable at %s", bridgedb_url
        )
        sys.exit(1)
    logger.info("BridgeDb pre-flight check... OK")

    results = []
    for idx, (version, gz_path) in enumerate(versions_to_process, start=1):
        version_dir = Path(gz_path).parent
        logger.info("[%02d/%02d] %s starting", idx, total, version)

        # Each version gets its own PipelineConfig to avoid cross-version
        # output collisions (stage functions write to config.data_dir).
        config = PipelineConfig(
            data_dir=version_dir,
            bridgedb_url=bridgedb_url,
            request_timeout=30,
            log_level="WARNING",
        )

        result = process_version(gz_path, version, config, args.force)
        results.append(result)

        if result.status == "PASS":
            logger.info("[%02d/%02d] %s PASS (%.1fs)", idx, total, version, result.duration)
        elif result.status == "SKIP":
            logger.info("[%02d/%02d] %s SKIP (all 4 TTL files exist)", idx, total, version)
        elif result.status == "INVALID":
            logger.error(
                "[%02d/%02d] %s INVALID (%.1fs): %s",
                idx, total, version, result.duration, result.error,
            )
        else:
            logger.error(
                "[%02d/%02d] %s FAIL (%.1fs): %s",
                idx, total, version, result.duration, result.error,
            )

    _print_summary(results)

    failed = [r for r in results if r.status in ("FAIL", "INVALID")]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
