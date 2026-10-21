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
| `scripts/add-quarter.sh` | Load one quarter onto production (the cluster) |
| `load.sh` | Load TTLs into a **local** Virtuoso's named graphs (+ catalogue/SD into a metadata graph) |
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
3. **Load onto the cluster (manual):** `scripts/add-quarter.sh <date> --run <run-id>`,
   see "Cluster load" below.

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

<!-- runbook:start -->
Loading a quarter onto production is one command, run from the root of a
checkout of this repo on your own machine after the quarter's PR is merged:

```bash
scripts/add-quarter.sh <date> --run <run-id>
```

It needs `gh`, the Setup Python requirements (for the catalogue) and `ssh tgx1`.
It asks before it writes, then:

1. downloads the four TTLs (main, Genes, Enriched, Void) from the workflow
   artifact `aopwikirdf-<date>` of run `<run-id>` (kept 30 days; without
   `--run` it takes the newest run that has one, or use `--from DIR`);
2. regenerates the version catalogue + service description from `versions.txt`;
3. uploads both to `/mnt/gluster/docker/aopwiki-dashboard/staging/` and does the
   rest with one-off swarm services, so it does not matter which node runs
   Virtuoso: copies the TTLs into its `/database/data` (they stay there, the full
   reload in the dashboard repo rebuilds from that folder), `TTLP`s each into
   `http://aopwiki.org/graph/<date>`, reloads the metadata graph and checkpoints,
   with isql and the dba password from the swarm secret `virtuoso_dba_password`;
4. checks the new graph: AOPs, KEs, KERs and stressors present, and none below
   90% of the previous quarter;
5. starts the dashboard's query-store build; the quarter shows up there when it
   publishes its manifest (about 7 minutes).

It refuses a graph that already has triples (loading twice would duplicate
blank nodes) unless `--replace`, and it never touches other graphs. Virtuoso and
the dashboard are not restarted.

`--dry-run` shows the steps and runs only read queries. A test load that leaves
the live graphs alone goes into a graph outside `http://aopwiki.org/graph/`,
which also skips the catalogue and the store build:

```bash
scripts/add-quarter.sh <date> --graph http://aopwiki.org/test-graph/<date> --clear-after
```

For a quarter that is already live, the test load must match its counts exactly.
<!-- runbook:end -->

Never run a global reset (`RDF_GLOBAL_RESET`, `./load.sh --full`) against
production; those are for a host you brought up yourself. Rebuilding every graph
from the staged files is `scripts/reload-virtuoso.sh` in the dashboard repo.

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

## Backups

The production database is backed up every night without stopping it
(dashboard #162). Two swarm cron jobs run the scripts in `scripts/`, in the
Virtuoso image, on whichever manager is free:

| Job | Schedule (UTC) | Does |
|---|---|---|
| `cronjobs_aopwiki-dashboard-backup` | daily 01:30 | `backup-virtuoso.sh`: `backup_online()` of the whole database, packed with `virtuoso.ini` into `/mnt/hidrive/docker/aopwiki-dashboard/virtuoso/virtuoso-<stamp>.tar.gz` (+ `.sha256`, + `.manifest` with the graph and triple counts). Keeps 14 on HiDrive and the last 2 unpacked in `virtuoso-data/backups/`. |
| `cronjobs_aopwiki-dashboard-backup-verify` | Sundays 02:30 | `verify-virtuoso-backup.sh`: restores the newest archive into a throwaway Virtuoso inside the job's own container and fails unless every graph comes back with its manifest count. |

A backup is about 21 MB and takes a few seconds. The scripts are mounted from
`/mnt/gluster/docker/aopwiki-dashboard/jobs/`; after changing one here, copy it
there:

```bash
scp scripts/backup-virtuoso.sh scripts/verify-virtuoso-backup.sh \
    tgx1:/mnt/gluster/docker/aopwiki-dashboard/jobs/
```

Run either job now with
`docker service update --detach --force --replicas 1 cronjobs_aopwiki-dashboard-backup`
and read the result with `docker service logs --raw <job>`.

**Restoring production** (the database is lost or corrupt):

1. Stop Virtuoso cleanly: `docker service scale aopwiki-dashboard_virtuoso=0`
   (its stop grace period is 300 s; an interrupted checkpoint is what corrupted
   it in October 2026).
2. In a one-off root container with `virtuoso-data` mounted at `/database`, move
   `virtuoso.db`, `virtuoso.trx` and `virtuoso-temp.db` into a `corrupt-<date>/`
   folder (keep them), unpack the chosen archive into `/database/restore/`, copy
   its `aopwiki-*.bp` into `/database`, and run
   `cd /database && /opt/virtuoso-opensource/bin/virtuoso-t +configfile virtuoso.ini +restore-backup aopwiki-`.
   Then remove the copied `.bp` files.
3. `docker service scale aopwiki-dashboard_virtuoso=1`, and compare the graph
   counts with the archive's `graphs.txt`.
4. A quarter loaded after that backup is missing: load it again with
   `scripts/add-quarter.sh`. Then restart the dashboard so it re-reads the
   versions, and run the query-store build.

## Local development

```bash
cd Setup
python setup_versions.py     # download all snapshots in versions.txt
python generate_all_rdf.py   # convert all → versions/<date>/*.ttl
docker compose up -d          # local Virtuoso (8890 SPARQL, 1111 isql localhost)
./copy_data_files.sh          # versions/ -> aopwikirdf/ (+ catalogue)
./load.sh                     # TTLP the quarters whose graph is still empty
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
