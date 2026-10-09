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
| `generate_catalog.py` | Emit the DCAT version catalogue + SPARQL service description |
| `load.sh` | Load TTLs into Virtuoso named graphs (+ catalogue/SD into a metadata graph) |
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

This is the one canonical procedure for adding a quarter to production. It is
incremental: it adds one named graph and never resets the store. The Virtuoso
runs as swarm service `aopwiki-dashboard_virtuoso` (stack in the dashboard repo)
and is not node-pinned, so run the `docker cp` steps on whichever node hosts it
(`docker service ps aopwiki-dashboard_virtuoso`).

1. **Fetch the TTLs** from the PR's workflow run (artifacts expire after 30 days):
   ```bash
   gh run download <run-id> -R marvinm2/AOP-Wiki_multi-endpoint -n aopwikirdf-<date> -D q-<date>
   scp q-<date>/AOPWikiRDF-*.ttl tgx1:staging/<date>/
   ```
2. **Stage all four files** (main, Genes, Enriched, Void) in the data mount. Your
   user can't write it directly, so copy through the container:
   ```bash
   C=$(docker ps -q -f name=aopwiki-dashboard_virtuoso)
   for f in ~/staging/<date>/AOPWikiRDF-*.ttl; do docker cp "$f" "$C":/database/data/; done
   ```
   Keep every quarter's four files there: the full reload in the dashboard repo
   (`scripts/reload-virtuoso.sh`) rebuilds from this directory.
3. **Load into the dated graph.** Put one statement per line in a SQL file (isql
   rejects several statements on one line):
   ```sql
   log_enable(2);
   DB.DBA.TTLP(file_to_string_output('/database/data/AOPWikiRDF-<date>.ttl'), '', 'http://aopwiki.org/graph/<date>');
   -- …one TTLP per file (Genes, Enriched, Void)…
   checkpoint;
   SPARQL SELECT COUNT(*) FROM <http://aopwiki.org/graph/<date>> WHERE {?s ?p ?o};
   ```
   Run it with isql from a one-off service that mounts the dba password from the
   swarm secret `virtuoso_dba_password`; the procedure is in the cluster service
   doc (`services/aopwiki-dashboard.md`, "Data loading").
4. **Merge the PR**, then regenerate the version catalogue and service description
   (`python generate_catalog.py --out <dir>`), stage both files the same way, and
   reload the metadata graph: `SPARQL CLEAR GRAPH <http://aopwiki-multirdf.vhp4safety.nl/metadata>;`
   followed by a TTLP of `AOPWikiRDF-Catalog.ttl` and `ServiceDescription.ttl` into it.
5. **Restart the dashboard** so it picks up the new latest version:
   `docker service update --force aopwiki-dashboard_dashboard`. Virtuoso itself does
   not need a restart.
6. **Verify** (below): the AOP/KE/KER/Stressor counts in the new graph match the
   PR's stats table, and the catalogue's newest `owl:versionInfo` equals the newest graph.

Never run a global reset (`RDF_GLOBAL_RESET`, `./load.sh --full`) against
production; those are for a host you brought up yourself.

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

The `endpoint-health` workflow runs daily and opens an issue if the graph count
drops below the floor, if the newest graph is more than 21 days behind the most
recent quarter start, or if the version catalogue lags the graphs.

## Local development

```bash
cd Setup
python setup_versions.py     # download all snapshots in versions.txt
python generate_all_rdf.py   # convert all → versions/<date>/*.ttl
docker compose up -d          # local Virtuoso (8890 SPARQL, 1111 isql localhost)
./load.sh                     # incremental load into dated graphs
```

`copy_data_files.sh` regenerates the version catalogue + service description, and
`load.sh` loads them into the metadata graph
`http://aopwiki-multirdf.vhp4safety.nl/metadata`. This makes the version series
machine-discoverable (DCAT `dcat:version`/`dcat:previousVersion` chain,
`owl:versionInfo`) and gives the endpoint a SPARQL 1.1 Service Description
listing every named graph. Query them with:

```sparql
# list all versions, newest first
SELECT ?v WHERE { GRAPH <http://aopwiki-multirdf.vhp4safety.nl/metadata> {
  ?d <http://www.w3.org/2002/07/owl#versionInfo> ?v } } ORDER BY DESC(?v)
```

Conversion logic lives in the `aopwiki-rdf` submodule (`Setup/aopwiki-rdf/`), not
in this repo. `requirements.txt` installs it editable.
