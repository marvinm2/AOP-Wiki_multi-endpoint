"""Generate machine-readable versioning + service metadata for the endpoint.

Emits two TTL files into the Virtuoso data dir so the multi-version nature of the
endpoint is FAIR and discoverable (it isn't, by default — versions are only
implicit in the graph URIs):

* ``AOPWikiRDF-Catalog.ttl`` — a DCAT catalogue describing every quarterly
  version as a dataset, with a ``dcat:previousVersion`` chain, ``dcat:version`` /
  ``owl:versionInfo`` / ``dcterms:issued`` per version, and a top-level dataset
  that advertises ``void:sparqlEndpoint`` and ``dcat:accrualPeriodicity``.
* ``ServiceDescription.ttl`` — a SPARQL 1.1 Service Description for the endpoint
  that lists every version as an ``sd:namedGraph`` (the bare endpoint currently
  returns an empty service description).

The version list comes from ``versions.txt`` (via setup_versions), so this needs
no network access. ``load.sh`` loads both files into a dedicated metadata graph.

Usage:
    python generate_catalog.py                 # writes into ./aopwikirdf
    python generate_catalog.py --out /tmp/x     # custom dir
"""
from __future__ import annotations

import argparse
from pathlib import Path

from rdflib import Graph, Literal, URIRef, BNode, Namespace, RDF
from rdflib.namespace import XSD, FOAF, DCTERMS, OWL

import setup_versions

DCAT = Namespace("http://www.w3.org/ns/dcat#")
VOID = Namespace("http://rdfs.org/ns/void#")
SD = Namespace("http://www.w3.org/ns/sparql-service-description#")
FREQ = Namespace("http://purl.org/cld/freq/")

ENDPOINT = "https://aopwiki-multirdf.vhp4safety.nl/sparql"
DASHBOARD = "https://aopwiki-dashboard.vhp4safety.nl"
GRAPH_BASE = "http://aopwiki.org/graph/"
LICENSE = URIRef("https://creativecommons.org/licenses/by-sa/4.0/")
CATALOG = URIRef("https://aopwiki-multirdf.vhp4safety.nl/#catalog")
DATASET = URIRef("https://aopwiki-multirdf.vhp4safety.nl/#dataset")

RESULT_FORMATS = [
    "http://www.w3.org/ns/formats/SPARQL_Results_JSON",
    "http://www.w3.org/ns/formats/SPARQL_Results_XML",
    "http://www.w3.org/ns/formats/SPARQL_Results_CSV",
    "http://www.w3.org/ns/formats/Turtle",
    "http://www.w3.org/ns/formats/N-Triples",
    "http://www.w3.org/ns/formats/RDF_XML",
]


def _bind(g: Graph) -> None:
    g.bind("dcat", DCAT)
    g.bind("void", VOID)
    g.bind("sd", SD)
    g.bind("freq", FREQ)
    g.bind("dcterms", DCTERMS)
    g.bind("owl", OWL)
    g.bind("foaf", FOAF)


def build_catalog(versions: list[str]) -> Graph:
    """DCAT catalogue with a per-version dataset chain."""
    versions = sorted(versions)
    g = Graph()
    _bind(g)

    # Top-level multi-version dataset
    g.add((DATASET, RDF.type, VOID.Dataset))
    g.add((DATASET, RDF.type, DCAT.Dataset))
    g.add((DATASET, DCTERMS.title, Literal("AOP-Wiki RDF (all versions)")))
    g.add((DATASET, DCTERMS.description,
           Literal("Every quarterly AOP-Wiki release in RDF, one dated named graph per version.")))
    g.add((DATASET, DCTERMS.license, LICENSE))
    g.add((DATASET, FOAF.homepage, URIRef(DASHBOARD)))
    g.add((DATASET, VOID.sparqlEndpoint, URIRef(ENDPOINT)))
    g.add((DATASET, DCAT.accrualPeriodicity, FREQ.quarterly))

    # Catalogue
    g.add((CATALOG, RDF.type, DCAT.Catalog))
    g.add((CATALOG, DCTERMS.title, Literal("AOP-Wiki multi-version RDF catalogue")))
    g.add((CATALOG, DCTERMS.publisher, URIRef("https://aopwiki-multirdf.vhp4safety.nl")))
    g.add((CATALOG, DCTERMS.license, LICENSE))
    g.add((CATALOG, FOAF.homepage, URIRef(DASHBOARD)))

    prev = None
    for v in versions:
        ds = URIRef(f"{GRAPH_BASE}{v}")
        g.add((ds, RDF.type, VOID.Dataset))
        g.add((ds, RDF.type, DCAT.Dataset))
        g.add((ds, DCTERMS.title, Literal(f"AOP-Wiki RDF {v}")))
        g.add((ds, DCTERMS.issued, Literal(v, datatype=XSD.date)))
        g.add((ds, DCTERMS.modified, Literal(v, datatype=XSD.date)))
        g.add((ds, DCAT.version, Literal(v)))
        g.add((ds, OWL.versionInfo, Literal(v)))
        g.add((ds, VOID.sparqlEndpoint, URIRef(ENDPOINT)))
        g.add((ds, DCTERMS.license, LICENSE))
        g.add((ds, DCTERMS.isPartOf, DATASET))
        if prev is not None:
            g.add((ds, DCAT.previousVersion, prev))
        g.add((DATASET, DCTERMS.hasVersion, ds))
        g.add((DATASET, VOID.subset, ds))
        g.add((CATALOG, DCAT.dataset, ds))
        prev = ds

    # Point the dataset's "latest" at the newest version
    if versions:
        g.add((DATASET, DCAT.version, Literal(versions[-1])))
    return g


def build_service_description(versions: list[str]) -> Graph:
    """SPARQL 1.1 Service Description advertising every version as a named graph."""
    g = Graph()
    _bind(g)
    svc = URIRef(ENDPOINT)
    g.add((svc, RDF.type, SD.Service))
    g.add((svc, SD.endpoint, URIRef(ENDPOINT)))
    g.add((svc, SD.supportedLanguage, SD.SPARQL11Query))
    for fmt in RESULT_FORMATS:
        g.add((svc, SD.resultFormat, URIRef(fmt)))
    g.add((svc, SD.feature, SD.UnionDefaultGraph))
    g.add((svc, SD.feature, SD.BasicFederatedQuery))
    g.add((svc, DCTERMS.title, Literal("AOP-Wiki multi-version SPARQL endpoint")))
    g.add((svc, DCTERMS.description,
           Literal("SPARQL endpoint serving every quarterly AOP-Wiki release as a dated named graph.")))

    ds = BNode()
    g.add((svc, SD.defaultDataset, ds))
    g.add((ds, RDF.type, SD.Dataset))
    g.add((ds, DCTERMS.title, Literal("AOP-Wiki multi-version RDF")))
    for v in sorted(versions):
        ng = BNode()
        g.add((ds, SD.namedGraph, ng))
        g.add((ng, RDF.type, SD.NamedGraph))
        g.add((ng, SD.name, URIRef(f"{GRAPH_BASE}{v}")))
    return g


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default="aopwikirdf", help="Output directory (default: aopwikirdf).")
    args = parser.parse_args()

    versions = setup_versions.load_versions()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    cat = build_catalog(versions)
    sd = build_service_description(versions)
    cat_path = out / "AOPWikiRDF-Catalog.ttl"
    sd_path = out / "ServiceDescription.ttl"
    cat.serialize(destination=str(cat_path), format="turtle")
    sd.serialize(destination=str(sd_path), format="turtle")

    print(f"Wrote {cat_path} ({len(cat)} triples) and {sd_path} ({len(sd)} triples) "
          f"for {len(versions)} versions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
