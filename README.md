# AOP-Wiki Multi-Version SPARQL Endpoint

A Virtuoso-backed SPARQL endpoint that holds **every quarterly AOP-Wiki snapshot** from 2018-04-01 through the present as separately-named RDF graphs. Useful for historical comparisons, reproducibility of regulatory queries against a fixed snapshot, and trend analysis of the AOP knowledge base over time.

**Live endpoint:** [https://aopwiki-multirdf.vhp4safety.nl/sparql](https://aopwiki-multirdf.vhp4safety.nl/sparql)

Each version is loaded under a graph URI of the form `http://aopwiki.org/graph/YYYY-MM-DD`, so federated and trend queries can simply pin the `GRAPH` clause to the date of interest.

## Pipeline

```
aopwiki.org/downloads/aop-wiki-xml-YYYY-MM-DD.gz
        │ setup_versions.py
        ▼
versions/YYYY-MM-DD/aop-wiki-xml-YYYY-MM-DD.gz
        │ generate_all_rdf.py  (uses the aopwiki_rdf Python package)
        ▼
aopwikirdf/AOPWikiRDF-YYYY-MM-DD.ttl   (+ -Genes-, -Void-, -Enriched- variants)
        │ load.sh + Virtuoso (docker-compose.yml)
        ▼
Named graph http://aopwiki.org/graph/YYYY-MM-DD
```

## Quick start

```bash
# 1. Download every quarterly XML snapshot
python setup_versions.py

# 2. Convert each version to TTL (modular pipeline from the aopwiki_rdf package)
python generate_all_rdf.py

# 3. Bring up Virtuoso (defaults defined in docker-compose.yml)
cp .env.example .env  # edit DBA_PASSWORD before first start
docker compose up -d

# 4. Load every TTL into its dated named graph
./load.sh
```

## Repository layout

| File | Purpose |
|---|---|
| `setup_versions.py` | Downloads the quarterly XML snapshots into `versions/`. |
| `generate_all_rdf.py` | Batch-runs the conversion across every version. |
| `validate_rdf.py` | Standalone TTL validation gate. |
| `load.sh` | Loads each TTL into Virtuoso under its dated named graph. |
| `copy_data_files.sh` | Copies generated TTLs into the Virtuoso mount. |
| `docker-compose.yml` | Virtuoso 7 OpenSource service (4 GB mem limit, healthcheck via `wget`). |
| `aopwikirdf/` | TTL outputs per version, mounted into Virtuoso at `/database/data`. |
| `versions/` | Downloaded XML snapshots, one folder per date. |
| `test_compatibility.py` | Multi-version XML schema compatibility test. |

The conversion logic itself lives in the [`AOPWikiRDF`](https://github.com/marvinm2/AOPWikiRDF) repository (Python package `aopwiki_rdf`); this repository is the **multi-version orchestration and deployment** layer.

## Maintainers

- **Lead maintainer:** Marvin Martens — Department of Translational Genomics, Maastricht University — [ORCID 0000-0003-2230-0840](https://orcid.org/0000-0003-2230-0840)
- **Backup maintainer:** Egon Willighagen — Department of Translational Genomics, Maastricht University — [ORCID 0000-0001-7542-0286](https://orcid.org/0000-0001-7542-0286)

For questions, bug reports, and feature requests please open a [GitHub Issue](https://github.com/marvinm2/AOP-Wiki_multi-endpoint/issues).

## License

- **This repository's code** (Python pipeline, shell scripts, docker-compose configuration): MIT — see [`LICENSE`](LICENSE).
- **Served AOP-Wiki RDF dataset**: Creative Commons Attribution-ShareAlike 4.0 International (CC-BY-SA 4.0) — see the [AOPWikiRDF data licence](https://github.com/marvinm2/AOPWikiRDF/blob/master/data/LICENSE-DATA). Matches the upstream [AOP-Wiki](https://aopwiki.org/) content licence.

## Citation

If you use the AOP-Wiki RDF data served by this endpoint, please cite:

Martens M., Evelo C.T., Willighagen E.L. (2022). *Providing Adverse Outcome Pathways from the AOP-Wiki in a Semantic Web Format to Increase Usability and Accessibility of the Content.* Applied In Vitro Toxicology 8(1):2–13. [doi:10.1089/aivt.2021.0010](https://doi.org/10.1089/aivt.2021.0010)
