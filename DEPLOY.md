# Deploy & quarterly-update runbook

How the multi-version AOP-Wiki RDF endpoint is built, updated each quarter, and
loaded onto the live cluster. The live endpoint is **production** — loading new
data is always a deliberate manual step. CI never writes to it.

- Live SPARQL endpoint: https://aopwiki-multirdf.vhp4safety.nl/sparql
- Named-graph contract: `http://aopwiki.org/graph/YYYY-MM-DD` (one per quarter)
- Repo: `marvinm2/AOP-Wiki_multi-endpoint`

## Components

| Piece | What it does |
|---|---|
| `versions.txt` | Source-of-truth list of quarters (one date per line) |
| `setup_versions.py` | Downloads snapshots; quarter detection helpers |
| `add_version.py` | One quarter: download → convert → validate → stats diff |
| `generate_all_rdf.py` | Batch convert all versions (`aopwiki-rdf` submodule pipeline) |
| `validate_rdf.py` | TTL syntax + entity-count gate |
| `load.sh` | Load TTLs into Virtuoso named graphs |
| `.github/workflows/quarterly-update.yml` | Detects a new quarter, opens a PR |
| `.github/workflows/rdf-validation.yml` | Guards the validator (fixtures) |
| `.github/workflows/endpoint-health.yml` | Daily live-endpoint health check |

## Quarterly update (the normal path)

1. **Automated detection (Mondays / manual dispatch).** `quarterly-update.yml`
   runs `add_version.py --detect`. When a new quarter is published it converts +
   validates it, attaches the TTLs as a run artifact, registers the date in
   `versions.txt`, and opens a **PR** with the stats diff + this runbook in the
   body. You receive the PR-opened email (you're requested as reviewer).
2. **Review & merge.** Check the stats diff looks sane (no collapse in entity
   counts), then merge the PR. Merging only records the version — it does **not**
   touch the endpoint.
3. **Load onto the cluster (manual).** See "Cluster load" below.

### Doing it by hand locally

```bash
cd Setup
cp .env.example .env                       # set DBA_PASSWORD before first start
python add_version.py 2026-07-01 \         # or --detect first
    --bridgedb-url https://webservice.bridgedb.org/Human \
    --stats-out stats.md
# → versions/2026-07-01/AOPWikiRDF-2026-07-01.ttl (+ -Genes/-Void/-Enriched)
```

## Cluster load (manual, production)

The dashboard `stack.yml` deploys both Virtuoso and the dashboard; TTLs live on
the gluster-backed virtuoso data mount.

```bash
ssh tgx1
cd ~/aopwiki-dashboard
# Copy the new version's TTLs into the virtuoso data mount (from the PR artifact
# or by generating them on a host with BridgeDb), then:
./load.sh --incremental                    # loads only graphs not already present
docker service update --force aopwiki-dashboard_virtuoso
```

`./load.sh --full --yes` wipes and reloads everything — only on a host you brought
up yourself, never casually against production.

## Verify after loading

```sparql
# New named graph present and populated?
SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE {
  GRAPH <http://aopwiki.org/graph/2026-07-01> { ?s a <http://aopkb.org/aop_ontology#AdverseOutcomePathway> }
}
```

```bash
# Total named graphs (should be the versions.txt count)
curl -s --get https://aopwiki-multirdf.vhp4safety.nl/sparql \
  --data-urlencode 'query=SELECT (COUNT(DISTINCT ?g) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } FILTER(STRSTARTS(STR(?g),"http://aopwiki.org/graph/")) }' \
  -H 'Accept: application/sparql-results+json'
```

The `endpoint-health` workflow runs this check daily and opens an issue if the
graph count drops below the floor.

## Local development

```bash
cd Setup
python setup_versions.py     # download all snapshots in versions.txt
python generate_all_rdf.py   # convert all → versions/<date>/*.ttl
docker compose up -d          # local Virtuoso (8890 SPARQL, 1111 isql localhost)
./load.sh                     # incremental load into dated graphs
```

Conversion logic lives in the `aopwiki-rdf` submodule (`Setup/aopwiki-rdf/`), not
in this repo. `requirements.txt` installs it editable.
