"""Standalone TTL validation module for AOP-Wiki RDF pipeline.

Can be used as:
- A standalone CLI: python validate_rdf.py versions/2025-07-01/
- An importable module: from validate_rdf import validate_ttl_file, validate_version_dir

Public API:
    validate_ttl_file(path)        -> TtlValidationResult
    validate_version_dir(...)      -> VersionValidationResult
    TtlValidationResult            (dataclass)
    VersionValidationResult        (dataclass)
"""

import argparse
import dataclasses
import glob
import logging
import sys
from pathlib import Path
from typing import Optional

from rdflib import Graph, URIRef
from rdflib.namespace import RDF

logger = logging.getLogger(__name__)

# Confirmed namespace URIs from aopwiki-rdf/src/aopwiki_rdf/rdf/namespaces.py
_NS_AOPO = "http://aopkb.org/aop_ontology#"
_NS_NCI  = "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#"

_URI_AOP        = URIRef(_NS_AOPO + "AdverseOutcomePathway")
_URI_KE         = URIRef(_NS_AOPO + "KeyEvent")
_URI_KER        = URIRef(_NS_AOPO + "KeyEventRelationship")
_URI_STRESSOR   = URIRef(_NS_NCI  + "C54571")

# Unversioned stage output filenames (written before atomic rename).
# Entity-count check applies ONLY to the main file; the others would return
# zero entity counts and trigger false positives.
_STAGE_FILES = [
    "AOPWikiRDF.ttl",
    "AOPWikiRDF-Enriched.ttl",
    "AOPWikiRDF-Genes.ttl",
    "AOPWikiRDF-Void.ttl",
]
_MAIN_FILE = "AOPWikiRDF.ttl"


@dataclasses.dataclass
class TtlValidationResult:
    """Per-file validation result."""
    path: Path
    valid: bool
    triple_count: int
    error: Optional[str] = None
    graph: Optional[Graph] = dataclasses.field(default=None, repr=False)


@dataclasses.dataclass
class VersionValidationResult:
    """Per-version aggregate validation result."""
    version: str
    valid: bool
    total_triples: int
    file_results: list
    entity_error: Optional[str] = None


def validate_ttl_file(path: Path) -> TtlValidationResult:
    """Parse a Turtle file and check it is syntactically valid and non-empty.

    Parameters
    ----------
    path:
        Path to the .ttl file to validate.

    Returns
    -------
    TtlValidationResult with valid=True if the file parses successfully and
    contains at least one triple; valid=False with an error message otherwise.
    """
    path = Path(path)
    g = Graph()
    try:
        g.parse(str(path), format="turtle")
    except Exception as exc:
        return TtlValidationResult(
            path=path,
            valid=False,
            triple_count=0,
            error=f"parse error: {exc}",
        )

    triple_count = len(g)
    if triple_count == 0:
        return TtlValidationResult(
            path=path,
            valid=False,
            triple_count=0,
            error="zero triples -- empty or header-only file",
        )

    return TtlValidationResult(
        path=path,
        valid=True,
        triple_count=triple_count,
        graph=g,
    )


def _check_entity_counts(g: Graph) -> Optional[str]:
    """Check that the main TTL graph contains non-zero AOPs, KEs, KERs, and Stressors.

    Parameters
    ----------
    g:
        Parsed rdflib Graph (should be the main AOPWikiRDF graph).

    Returns
    -------
    An error string describing which entity type(s) are missing, or None if all
    counts are non-zero.
    """
    counts = {
        "AOPs":       len(list(g.subjects(RDF.type, _URI_AOP))),
        "KEs":        len(list(g.subjects(RDF.type, _URI_KE))),
        "KERs":       len(list(g.subjects(RDF.type, _URI_KER))),
        "Stressors":  len(list(g.subjects(RDF.type, _URI_STRESSOR))),
    }
    zero_types = [name for name, count in counts.items() if count == 0]
    if zero_types:
        return f"zero entities for: {', '.join(zero_types)}"
    return None


def _find_main_file(version_dir: Path) -> Optional[Path]:
    """Return the main TTL file path (unversioned or versioned name).

    Checks for the unversioned stage name first, then falls back to globbing
    for the versioned name.  Returns None if no match is found.
    """
    unversioned = version_dir / _MAIN_FILE
    if unversioned.exists():
        return unversioned
    # Versioned name pattern: AOPWikiRDF-YYYY-MM-DD.ttl
    # Exclude -Enriched, -Genes, -Void variants.
    matches = [
        p for p in version_dir.glob("AOPWikiRDF-*.ttl")
        if not any(
            p.name.startswith(prefix)
            for prefix in ("AOPWikiRDF-Enriched-", "AOPWikiRDF-Genes-", "AOPWikiRDF-Void-")
        )
    ]
    return matches[0] if len(matches) == 1 else None


def _resolve_ttl_path(version_dir: Path, stage_name: str) -> Optional[Path]:
    """Resolve a stage filename to an existing file (unversioned or versioned).

    For unversioned: returns the path if it exists.
    For versioned fallback: strips the base name and globs for dated variants.
    Returns None if no file is found.
    """
    unversioned = version_dir / stage_name
    if unversioned.exists():
        return unversioned

    # Derive glob pattern for the versioned variant.
    # e.g. "AOPWikiRDF.ttl" -> "AOPWikiRDF-*.ttl"
    # e.g. "AOPWikiRDF-Enriched.ttl" -> "AOPWikiRDF-Enriched-*.ttl"
    base = stage_name.removesuffix(".ttl")
    matches = list(version_dir.glob(f"{base}-*.ttl"))
    return matches[0] if len(matches) == 1 else None


def validate_version_dir(
    version_dir: Path,
    aop_count: int = -1,
    ke_count: int = -1,
    ker_count: int = -1,
    stressor_count: int = -1,
) -> VersionValidationResult:
    """Validate all TTL files in a version directory.

    Checks syntax and triple count for each of the 4 expected TTL files, then
    verifies entity counts are non-zero either from the provided counts (integrated
    pipeline path) or by SPARQL on the parsed main TTL (standalone path).

    Parameters
    ----------
    version_dir:
        Directory containing the TTL files (the version's data_dir).
    aop_count, ke_count, ker_count, stressor_count:
        Entity counts from the XML parser (>= 0).  Pass -1 (default) to have
        entity counts determined by parsing the main TTL file with rdflib.

    Returns
    -------
    VersionValidationResult with valid=True only if ALL checks pass.
    """
    version_dir = Path(version_dir)
    version = version_dir.name

    file_results = []
    overall_valid = True
    total_triples = 0
    main_graph: Optional[Graph] = None

    for stage_name in _STAGE_FILES:
        resolved = _resolve_ttl_path(version_dir, stage_name)
        if resolved is None:
            file_results.append(TtlValidationResult(
                path=version_dir / stage_name,
                valid=False,
                triple_count=0,
                error=f"missing file: {stage_name}",
            ))
            overall_valid = False
            continue

        result = validate_ttl_file(resolved)
        file_results.append(result)
        total_triples += result.triple_count

        if not result.valid:
            overall_valid = False

        # Retain the main TTL graph for entity-count checking (standalone mode).
        if stage_name == _MAIN_FILE and result.graph is not None:
            main_graph = result.graph

    # Entity-count check
    entity_error: Optional[str] = None

    # Integrated path: counts already known from XML parsing.
    if aop_count >= 0 and ke_count >= 0 and ker_count >= 0 and stressor_count >= 0:
        counts = {
            "AOPs":      aop_count,
            "KEs":       ke_count,
            "KERs":      ker_count,
            "Stressors": stressor_count,
        }
        zero_types = [name for name, count in counts.items() if count == 0]
        if zero_types:
            entity_error = f"zero entities for: {', '.join(zero_types)}"
    else:
        # Standalone path: parse the main TTL and count via rdflib SPARQL.
        if main_graph is None:
            # Try to find and parse the main file explicitly.
            main_path = _find_main_file(version_dir)
            if main_path is not None:
                ttl_result = validate_ttl_file(main_path)
                if ttl_result.valid and ttl_result.graph is not None:
                    main_graph = ttl_result.graph

        if main_graph is not None:
            entity_error = _check_entity_counts(main_graph)
        else:
            entity_error = "cannot determine entity counts: main TTL file not parseable"

    if entity_error:
        overall_valid = False

    return VersionValidationResult(
        version=version,
        valid=overall_valid,
        total_triples=total_triples,
        file_results=file_results,
        entity_error=entity_error,
    )


def _run_cli(paths: list) -> int:
    """Validate one or more paths (directories or .ttl files).

    Returns 0 if all are valid, 1 if any are invalid.
    """
    all_valid = True

    for raw_path in paths:
        p = Path(raw_path)

        if p.is_dir():
            result = validate_version_dir(p)
            if result.valid:
                logger.info(
                    "VALID   %s — %d triples across %d files",
                    p, result.total_triples, len(result.file_results),
                )
                for fr in result.file_results:
                    logger.info("  OK   %s (%d triples)", fr.path.name, fr.triple_count)
            else:
                all_valid = False
                logger.error("INVALID %s", p)
                for fr in result.file_results:
                    if fr.error:
                        logger.error("  FAIL %s: %s", fr.path.name, fr.error)
                    else:
                        logger.info("  OK   %s (%d triples)", fr.path.name, fr.triple_count)
                if result.entity_error:
                    logger.error("  ENTITY CHECK: %s", result.entity_error)

        elif p.suffix.lower() == ".ttl" or p.is_file():
            result = validate_ttl_file(p)
            if result.valid:
                logger.info("VALID   %s (%d triples)", p, result.triple_count)
            else:
                all_valid = False
                logger.error("INVALID %s: %s", p, result.error)

        else:
            logger.error("Path not found or not a directory/file: %s", p)
            all_valid = False

    return 0 if all_valid else 1


def _build_cli_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Validate AOP-Wiki RDF TTL files. "
            "Accepts one or more paths that can be version directories or individual .ttl files. "
            "Exits 0 if all valid, 1 if any are invalid."
        )
    )
    p.add_argument(
        "paths",
        nargs="+",
        metavar="PATH",
        help="Version directory or individual .ttl file to validate.",
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return p


if __name__ == "__main__":
    args = _build_cli_parser().parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
        handlers=[logging.StreamHandler()],
    )
    sys.exit(_run_cli(args.paths))
