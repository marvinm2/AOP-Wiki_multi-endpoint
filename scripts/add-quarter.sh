#!/usr/bin/env bash
#
# Load one AOP-Wiki quarter into the production multi-version endpoint (#22).
#
#   scripts/add-quarter.sh DATE [options]
#
# One command for what DEPLOY.md used to list as eight manual steps:
#   1. get the quarter's four TTLs (main, Genes, Enriched, Void) from the
#      quarterly-update workflow artifact, or from --from DIR;
#   2. regenerate the version catalogue + service description (generate_catalog.py);
#   3. upload all of it to a staging folder on GlusterFS, which every node sees;
#   4. on the cluster, with one-off swarm services so it does not matter which
#      node runs Virtuoso:
#      - copy the TTLs into Virtuoso's /database/data (kept there: the full
#        reload in the dashboard repo rebuilds from that folder);
#      - TTLP each file into http://aopwiki.org/graph/DATE, then checkpoint
#        (isql with the dba password from the swarm secret, never printed);
#      - reload the metadata graph from the new catalogue;
#   5. check the new graph: AOPs, KEs, KERs and stressors present, and none
#      below 90% of the previous quarter (a collapse means a broken conversion);
#   6. start the dashboard's query-store build, so the quarter shows up there.
#
# It never resets the store or touches other graphs. A graph that already has
# triples is refused unless --replace (TTLP twice would duplicate blank nodes).
#
# Options:
#   --run ID        quarterly-update run whose artifact aopwikirdf-DATE holds
#                   the TTLs (default: the newest run that has it)
#   --from DIR      take the four TTLs from DIR instead of an artifact
#   --graph IRI     load into IRI instead of http://aopwiki.org/graph/DATE.
#                   Anything else must not start with http://aopwiki.org/graph/;
#                   it skips the catalogue and the store build (a test load)
#   --clear-after   CLEAR the graph again at the end (with --graph, for a test)
#   --replace       CLEAR the graph first if it already has triples
#   --no-catalogue  leave the metadata graph alone
#   --no-store-build
#   --manager HOST  ssh alias of a swarm manager (default tgx1, or $AQ_MANAGER)
#   --dry-run       show what would be done; run only read queries
#   --yes           do not ask before writing to production
#
# Needs locally: gh (for --run), python with the Setup requirements (for the
# catalogue), ssh to a manager. Needs on the manager: docker, curl.
#
# Example test load, which leaves the live graphs alone:
#   scripts/add-quarter.sh 2026-10-01 --graph http://aopwiki.org/test-graph/2026-10-01 --clear-after
#
set -euo pipefail

REPO="marvinm2/AOP-Wiki_multi-endpoint"
GRAPH_BASE="http://aopwiki.org/graph/"
META_GRAPH="http://aopwiki-multirdf.vhp4safety.nl/metadata"
PUBLIC_ENDPOINT="https://aopwiki-multirdf.vhp4safety.nl/sparql"
VIRTUOSO_SERVICE="aopwiki-dashboard_virtuoso"
SERVICE_DIR="/mnt/gluster/docker/aopwiki-dashboard"
DATA_MOUNT="${SERVICE_DIR}/virtuoso-data"      # -> /database in the Virtuoso task
STAGING_ROOT="${SERVICE_DIR}/staging"
STORE_BUILD_JOB="cronjobs_aopwiki-dashboard-store-build"
SECRET="virtuoso_dba_password"
COLLAPSE_RATIO="0.9"

die() { echo "ERROR: $*" >&2; exit 1; }
say() { echo "==> $*"; }

usage() { sed -n '3,46p' "$0" | sed 's/^# \{0,1\}//'; }

# ---------------------------------------------------------------- arguments

DATE="" RUN_ID="" FROM_DIR="" GRAPH="" CLEAR_AFTER=false REPLACE=false
CATALOGUE=true STORE_BUILD=true MANAGER="${AQ_MANAGER:-tgx1}" DRY_RUN=false YES=false
REMOTE=false STAGING=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --run) RUN_ID="$2"; shift 2 ;;
        --from) FROM_DIR="$2"; shift 2 ;;
        --graph) GRAPH="$2"; shift 2 ;;
        --clear-after) CLEAR_AFTER=true; shift ;;
        --replace) REPLACE=true; shift ;;
        --no-catalogue) CATALOGUE=false; shift ;;
        --no-store-build) STORE_BUILD=false; shift ;;
        --manager) MANAGER="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        --yes) YES=true; shift ;;
        --remote) REMOTE=true; STAGING="$2"; shift 2 ;;   # internal: the cluster half
        -h|--help) usage; exit 0 ;;
        -*) die "unknown option $1 (see --help)" ;;
        *) [[ -z "$DATE" ]] || die "one DATE only"; DATE="$1"; shift ;;
    esac
done

[[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "DATE must be YYYY-MM-DD (see --help)"
CANONICAL="${GRAPH_BASE}${DATE}"
GRAPH="${GRAPH:-$CANONICAL}"
if [[ "$GRAPH" != "$CANONICAL" ]]; then
    # A test load: keep it out of the prefix the dashboard, the catalogue and
    # the query store read, and do not publish anything about it.
    [[ "$GRAPH" == "$GRAPH_BASE"* ]] && die "--graph under ${GRAPH_BASE} must be ${CANONICAL}"
    CATALOGUE=false
    STORE_BUILD=false
fi

FILES=("AOPWikiRDF-${DATE}.ttl" "AOPWikiRDF-Genes-${DATE}.ttl"
       "AOPWikiRDF-Enriched-${DATE}.ttl" "AOPWikiRDF-Void-${DATE}.ttl")
META_FILES=("AOPWikiRDF-Catalog.ttl" "ServiceDescription.ttl")

# ------------------------------------------------------- SPARQL (read-only)

sparql_json() {   # query -> results JSON, from the public endpoint
    curl -sf --get "$PUBLIC_ENDPOINT" --data-urlencode "query=$1" \
        -H 'Accept: application/sparql-results+json' --max-time 120
}

first_value() {   # JSON on stdin -> first value of variable $1 (or empty)
    python3 -c '
import json, sys
rows = json.load(sys.stdin)["results"]["bindings"]
print(rows[0][sys.argv[1]]["value"] if rows and sys.argv[1] in rows[0] else "")' "$1"
}

triples_in() {
    sparql_json "SELECT (COUNT(*) AS ?n) WHERE { GRAPH <$1> { ?s ?p ?o } }" | first_value n
}

entity_counts() {   # graph -> "AOPs KEs KERs Stressors"
    sparql_json "PREFIX aopo: <http://aopkb.org/aop_ontology#>
SELECT ?type (COUNT(DISTINCT ?s) AS ?n) WHERE {
  VALUES (?type ?class) { (\"AOPs\" aopo:AdverseOutcomePathway) (\"KEs\" aopo:KeyEvent)
                          (\"KERs\" aopo:KeyEventRelationship)
                          (\"Stressors\" <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C54571>) }
  GRAPH <$1> { ?s a ?class } } GROUP BY ?type" | python3 -c '
import json, sys
rows = {r["type"]["value"]: r["n"]["value"] for r in json.load(sys.stdin)["results"]["bindings"]}
print(" ".join(rows.get(t, "0") for t in ("AOPs", "KEs", "KERs", "Stressors")))'
}

previous_graph() {   # the newest dated graph before DATE
    sparql_json "SELECT ?g WHERE { GRAPH ?g { ?s a <http://aopkb.org/aop_ontology#AdverseOutcomePathway> }
  FILTER(STRSTARTS(STR(?g), \"${GRAPH_BASE}\") && STR(?g) < \"${CANONICAL}\") }
GROUP BY ?g ORDER BY DESC(?g) LIMIT 1" | first_value g
}

# ============================================================ cluster half

# One-off swarm service: wait until it has run, print its log, remove it.
run_once() {   # name, then docker service create arguments
    local name="$1"; shift
    docker service rm "$name" >/dev/null 2>&1 || true
    docker service create --detach --quiet --name "$name" --restart-condition none "$@" >/dev/null
    local state=""
    for _ in $(seq 1 360); do
        state=$(docker service ps "$name" --format '{{.CurrentState}}' 2>/dev/null | head -1)
        case "$state" in Complete*|Failed*|Rejected*|Shutdown*) break ;; esac
        sleep 5
    done
    sleep 2   # the log can lag the task state
    docker service logs --raw "$name" 2>/dev/null || true
    local error; error=$(docker service ps "$name" --no-trunc --format '{{.Error}}' 2>/dev/null | head -1)
    docker service rm "$name" >/dev/null 2>&1 || true
    [[ "$state" == Complete* ]] || die "$name ended as '${state:-unknown}' ${error}"
}

# Run an isql script through a one-off service with the dba password secret.
run_isql() {   # sql file, label
    local sql="$1" label="$2" image cfg out
    image=$(docker service inspect "$VIRTUOSO_SERVICE" --format '{{.Spec.TaskTemplate.ContainerSpec.Image}}')
    cfg="aq-${label}-$(date +%s)"
    docker config create "$cfg" "$sql" >/dev/null
    out=$(run_once "aq-isql-${label}" --network core --secret "$SECRET" \
            --config "source=${cfg},target=/load.sql" --entrypoint sh "$image" \
            -c "/opt/virtuoso-opensource/bin/isql ${VIRTUOSO_SERVICE}:1111 dba \"\$(cat /run/secrets/${SECRET})\" < /load.sql" \
          || { docker config rm "$cfg" >/dev/null 2>&1; exit 1; })
    docker config rm "$cfg" >/dev/null 2>&1 || true
    echo "$out" | grep -v '^$' | sed 's/^/    /'
    if echo "$out" | grep -q '\*\*\* Error'; then die "isql reported an error ($label)"; fi
}

remote_main() {
    [[ -d "$STAGING" ]] || die "no staging folder $STAGING"
    docker service inspect "$VIRTUOSO_SERVICE" >/dev/null || die "no service $VIRTUOSO_SERVICE"
    local image; image=$(docker service inspect "$VIRTUOSO_SERVICE" --format '{{.Spec.TaskTemplate.ContainerSpec.Image}}')

    say "Graph ${GRAPH}"
    local have; have=$(triples_in "$GRAPH")
    [[ -n "$have" ]] || die "could not count the triples of ${GRAPH}"
    if [[ "$have" != "0" ]] && ! $REPLACE; then
        die "${GRAPH} already has ${have} triples; use --replace to load it again"
    fi

    say "Copying the TTLs into the Virtuoso data folder"
    # Refuse to overwrite a different file of the same name; identical ones are skipped.
    local copy='set -e
for f in "$@"; do
  if [ -e "/database/data/$f" ]; then
    if [ "$(md5sum < "/staging/$f")" = "$(md5sum < "/database/data/$f")" ]; then echo "same: $f"; continue; fi
    case "$f" in AOPWikiRDF-Catalog.ttl|ServiceDescription.ttl) ;; *) [ -n "$REPLACE" ] || { echo "differs: $f (use --replace)"; exit 3; } ;; esac
  fi
  cp -p "/staging/$f" "/database/data/$f.part" && mv "/database/data/$f.part" "/database/data/$f" && echo "copied: $f"
done'
    local copy_files=("${FILES[@]}")
    $CATALOGUE && copy_files+=("${META_FILES[@]}")
    if $DRY_RUN; then
        printf '    would copy %s\n' "${copy_files[@]}"
    else
        # As root, like the Virtuoso task: other uids are refused on this gluster
        # mount whatever the folder modes say. cp -p keeps the uploader as owner.
        run_once "aq-copy-${DATE}" \
            -e "REPLACE=$($REPLACE && echo 1)" \
            --mount "type=bind,source=${STAGING},target=/staging,readonly" \
            --mount "type=bind,source=${DATA_MOUNT},target=/database" \
            --entrypoint sh "$image" -c "$copy" copy "${copy_files[@]}" | sed 's/^/    /'
    fi

    local sql="${STAGING}/load.sql"
    {
        echo "log_enable(2);"
        [[ "$have" != "0" ]] && echo "SPARQL CLEAR GRAPH <${GRAPH}>;"
        for f in "${FILES[@]}"; do
            echo "DB.DBA.TTLP(file_to_string_output('/database/data/${f}'), '', '${GRAPH}');"
        done
        if $CATALOGUE; then
            echo "SPARQL CLEAR GRAPH <${META_GRAPH}>;"
            for f in "${META_FILES[@]}"; do
                echo "DB.DBA.TTLP(file_to_string_output('/database/data/${f}'), '', '${META_GRAPH}');"
            done
        fi
        echo "checkpoint;"
    } > "$sql"
    say "Loading (isql)"
    if $DRY_RUN; then sed 's/^/    would run: /' "$sql"; else run_isql "$sql" "load-${DATE}"; fi

    say "Checking the new graph"
    if ! $DRY_RUN; then
        local now prev prev_counts
        now=$(entity_counts "$GRAPH")
        echo "    ${GRAPH}: AOPs KEs KERs Stressors = ${now}"
        # A test load of a quarter that is already live must match it exactly.
        if [[ "$GRAPH" != "$CANONICAL" && "$(triples_in "$CANONICAL")" != "0" ]]; then
            local live; live=$(entity_counts "$CANONICAL")
            echo "    ${CANONICAL} (live): AOPs KEs KERs Stressors = ${live}"
            [[ "$live" == "$now" ]] || die "the test load differs from the live ${DATE} graph"
            echo "    identical to the live graph"
        fi
        prev=$(previous_graph)
        if [[ -n "$prev" ]]; then
            prev_counts=$(entity_counts "$prev")
            echo "    ${prev}: AOPs KEs KERs Stressors = ${prev_counts}"
        fi
        python3 - "$now" "${prev_counts:-}" "$COLLAPSE_RATIO" <<'PY' || die "the new graph fails the count check (see above)"
import sys
now = [int(x) for x in sys.argv[1].split()]
prev = [int(x) for x in sys.argv[2].split()] if sys.argv[2] else None
ratio = float(sys.argv[3])
names = ("AOPs", "KEs", "KERs", "Stressors")
bad = [n for n, v in zip(names, now) if v == 0]
if prev:
    bad += [f"{n} {v} < {ratio:.0%} of {p}" for n, v, p in zip(names, now, prev) if v < ratio * p]
if bad:
    print("    FAIL: " + "; ".join(bad))
    sys.exit(1)
print("    OK")
PY
        if $CATALOGUE; then
            local newest
            newest=$(sparql_json "SELECT ?v WHERE { GRAPH <${META_GRAPH}> { ?d <http://www.w3.org/2002/07/owl#versionInfo> ?v } } ORDER BY DESC(?v) LIMIT 1" | first_value v)
            echo "    newest version in the catalogue: ${newest}"
            [[ "$newest" == "$DATE" ]] || echo "    WARNING: the catalogue's newest version is not ${DATE} (is ${DATE} the latest quarter?)"
        fi
    fi

    if $CLEAR_AFTER; then
        say "Clearing ${GRAPH} again"
        printf 'log_enable(2);\nSPARQL CLEAR GRAPH <%s>;\ncheckpoint;\n' "$GRAPH" > "${STAGING}/clear.sql"
        if $DRY_RUN; then sed 's/^/    would run: /' "${STAGING}/clear.sql"; else
            run_isql "${STAGING}/clear.sql" "clear-${DATE}"
            echo "    ${GRAPH} now has $(triples_in "$GRAPH") triples"
        fi
    fi

    if $STORE_BUILD; then
        say "Starting the dashboard's query-store build"
        if $DRY_RUN; then echo "    would run: docker service update --force --replicas 1 ${STORE_BUILD_JOB}"; else
            docker service update --detach --quiet --force --replicas 1 "$STORE_BUILD_JOB" >/dev/null
            echo "    started; the dashboard shows ${DATE} once it publishes a manifest (~7 min)"
        fi
    fi
    $DRY_RUN || rm -rf "$STAGING"
    say "Done"
}

if $REMOTE; then remote_main; exit 0; fi

# ============================================================== local half

SETUP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

if [[ -n "$FROM_DIR" ]]; then
    say "TTLs from ${FROM_DIR}"
    for f in "${FILES[@]}"; do cp "${FROM_DIR}/${f}" "$WORK/" 2>/dev/null || die "missing ${FROM_DIR}/${f}"; done
else
    command -v gh >/dev/null || die "gh is needed to fetch the workflow artifact (or use --from DIR)"
    if [[ -z "$RUN_ID" ]]; then
        RUN_ID=$(gh api "repos/${REPO}/actions/artifacts?name=aopwikirdf-${DATE}" \
                   -q '[.artifacts[] | select(.expired | not)] | sort_by(.created_at) | last | .workflow_run.id // empty')
        [[ -n "$RUN_ID" ]] || die "no unexpired artifact aopwikirdf-${DATE}; pass --from DIR"
    fi
    say "TTLs from run ${RUN_ID} (artifact aopwikirdf-${DATE})"
    gh run download "$RUN_ID" -R "$REPO" -n "aopwikirdf-${DATE}" -D "$WORK" >/dev/null
fi
for f in "${FILES[@]}"; do
    [[ -s "$WORK/$f" ]] || die "missing or empty: $f"
    python3 -c "import sys; sys.exit(0 if b'@prefix' in open(sys.argv[1], 'rb').read(65536) else 1)" "$WORK/$f" \
        || die "$f does not look like Turtle"
done
ls -l "$WORK"/*.ttl | awk '{printf "    %10d  %s\n", $5, $NF}' | sed "s#${WORK}/##"

if $CATALOGUE; then
    grep -qx "$DATE" "${SETUP_DIR}/versions.txt" \
        || die "${DATE} is not in versions.txt; merge the quarter's PR first (or --no-catalogue)"
    say "Generating the version catalogue"
    (cd "$SETUP_DIR" && python generate_catalog.py --out "$WORK" >/dev/null)
    for f in "${META_FILES[@]}"; do [[ -s "$WORK/$f" ]] || die "generate_catalog.py did not write $f"; done
fi

if ! $DRY_RUN && ! $YES; then
    echo "This writes ${GRAPH} on the production endpoint$($CATALOGUE && echo ' and reloads its version catalogue')."
    read -r -p "Continue? [y/N] " answer
    [[ "$answer" == [yY]* ]] || die "cancelled"
fi

STAGE="${STAGING_ROOT}/${DATE}-$(date +%Y%m%d%H%M%S)"
say "Uploading to ${MANAGER}:${STAGE}"
ssh "$MANAGER" "mkdir -p '$STAGE'"
scp -q "$WORK"/*.ttl "$0" "${MANAGER}:${STAGE}/"

ARGS=("$DATE" --remote "$STAGE" --graph "$GRAPH")
$CLEAR_AFTER && ARGS+=(--clear-after)
$REPLACE && ARGS+=(--replace)
$CATALOGUE || ARGS+=(--no-catalogue)
$STORE_BUILD || ARGS+=(--no-store-build)
$DRY_RUN && ARGS+=(--dry-run)
ssh "$MANAGER" "bash '${STAGE}/$(basename "$0")' $(printf '%q ' "${ARGS[@]}")"
$DRY_RUN && ssh "$MANAGER" "rm -rf '$STAGE'"
exit 0
