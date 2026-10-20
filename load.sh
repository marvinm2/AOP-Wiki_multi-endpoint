#!/bin/bash
set -euo pipefail
#
# Virtuoso Loader Script (multi-graph, versioned)
# Usage: ./load.sh [--full|--incremental] [--yes] [--help]
#
#   --full          Full reload: deletes all data, loads every quarter again.
#                   Requires confirmation unless --yes is also provided.
#   --incremental   (default) Load the quarters whose graph is still empty.
#
# For a LOCAL Virtuoso (docker compose). Production quarters are loaded with
# scripts/add-quarter.sh. Every file is loaded with DB.DBA.TTLP; the
# ld_dir/rdf_loader_run bulk loader was seen to load files partly on this
# setup without reporting it, so it is no longer used.
#   --yes           Skip confirmation prompt for --full mode.
#   --help, -h      Show this help text.
#
# Credentials are read from .env (DBA_PASSWORD).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ---------------------------------------------------------------------------
# usage()
# ---------------------------------------------------------------------------
usage() {
    cat <<'USAGE'
Usage: ./load.sh [OPTIONS]

Options:
  --full          Full reload (deletes all data, loads every quarter again).
                  Prompts for confirmation unless --yes is also given.
  --incremental   Load the quarters whose graph is still empty (default).
  --yes           Skip confirmation prompt for --full mode.
  --help, -h      Show this help text.

Credentials are read from .env (DBA_PASSWORD).
Old-style positional arguments (./load.sh load.log dba) are no longer supported.
USAGE
}

# ---------------------------------------------------------------------------
# Source .env
# ---------------------------------------------------------------------------
if [[ ! -f "${SCRIPT_DIR}/.env" ]]; then
    echo "ERROR: .env file not found at ${SCRIPT_DIR}/.env"
    echo "       Copy .env.example to .env and set DBA_PASSWORD."
    exit 1
fi
source "${SCRIPT_DIR}/.env"

if [[ "${DBA_PASSWORD:-}" == "dba" ]]; then
    echo "WARNING: DBA_PASSWORD is set to the default value 'dba'. Consider changing it."
fi

# ---------------------------------------------------------------------------
# CLI flag parser
# ---------------------------------------------------------------------------
MODE="incremental"
YES=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --full)
            MODE="full"
            shift
            ;;
        --incremental)
            MODE="incremental"
            shift
            ;;
        --yes)
            YES=true
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        -*)
            echo "ERROR: Unknown flag: $1"
            usage
            exit 1
            ;;
        *)
            echo "ERROR: Positional arguments are no longer supported."
            echo "  Old usage: ./load.sh <log_file> <password>"
            echo "  New usage: ./load.sh [--full|--incremental] [--yes]"
            echo "  Credentials are now read from .env (DBA_PASSWORD)."
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Full-mode confirmation
# ---------------------------------------------------------------------------
if [[ "$MODE" == "full" ]] && [[ "$YES" == "false" ]]; then
    echo "WARNING: --full mode will DELETE all RDF data from Virtuoso."
    read -r -p "This will DELETE all data. Continue? [y/N] " CONFIRM
    case "$CONFIRM" in
        y|Y) ;;
        *)
            echo "Aborted."
            exit 0
            ;;
    esac
fi

# ---------------------------------------------------------------------------
# Container health wait
# ---------------------------------------------------------------------------
CONTAINER="aopwiki-virtuoso"
echo "Waiting for ${CONTAINER} to become healthy..."
MAX_ITER=100
ITER=0
while true; do
    STATUS=$(docker inspect --format='{{.State.Health.Status}}' "${CONTAINER}" 2>/dev/null || echo "not_found")
    if [[ "$STATUS" == "healthy" ]]; then
        echo ""
        echo "${CONTAINER} is healthy."
        break
    fi
    ITER=$(( ITER + 1 ))
    if [[ $ITER -ge $MAX_ITER ]]; then
        echo ""
        echo "ERROR: ${CONTAINER} did not become healthy after $(( MAX_ITER * 3 )) seconds."
        exit 1
    fi
    printf "."
    sleep 3
done

# ---------------------------------------------------------------------------
# build_load_sql()
# One TTLP per file into http://aopwiki.org/graph/<date>, for every quarter in
# aopwikirdf/ (incremental: only quarters whose graph is empty), then the
# catalogue + service description into the metadata graph.
# ---------------------------------------------------------------------------
META_GRAPH="http://aopwiki-multirdf.vhp4safety.nl/metadata"

graph_is_empty() {   # graph IRI -> true if it has no triples on the local endpoint
    local answer
    answer=$(curl -sf --get "http://localhost:8890/sparql" \
          --data-urlencode "query=ASK { GRAPH <$1> { ?s ?p ?o } }" \
          -H 'Accept: application/sparql-results+json' | grep -o '"boolean" *: *[a-z]*' | grep -o '[a-z]*$')
    [[ "$answer" == "false" ]]
}

build_load_sql() {
    local container_data_dir="/database/data" version graph file
    for version in $(ls "${SCRIPT_DIR}/aopwikirdf/" | grep -oE '^AOPWikiRDF-[0-9]{4}-[0-9]{2}-[0-9]{2}\.ttl$' \
                     | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | sort -u); do
        graph="http://aopwiki.org/graph/${version}"
        if [[ "$MODE" == "incremental" ]] && ! graph_is_empty "$graph"; then
            echo "-- ${version}: already loaded"
            continue
        fi
        for file in "AOPWikiRDF-${version}.ttl" "AOPWikiRDF-Genes-${version}.ttl" \
                    "AOPWikiRDF-Enriched-${version}.ttl" "AOPWikiRDF-Void-${version}.ttl"; do
            [[ -f "${SCRIPT_DIR}/aopwikirdf/${file}" ]] || continue
            echo "DB.DBA.TTLP(file_to_string_output('${container_data_dir}/${file}'), '', '${graph}');"
        done
    done
    # The version catalogue + service description (generated by copy_data_files.sh)
    # are replaced as a whole, so the metadata graph never lists a stale set.
    if [[ -f "${SCRIPT_DIR}/aopwikirdf/AOPWikiRDF-Catalog.ttl" ]]; then
        echo "SPARQL CLEAR GRAPH <${META_GRAPH}>;"
        for file in "ServiceDescription.ttl" "AOPWikiRDF-Catalog.ttl"; do
            [[ -f "${SCRIPT_DIR}/aopwikirdf/${file}" ]] || continue
            echo "DB.DBA.TTLP(file_to_string_output('${container_data_dir}/${file}'), '', '${META_GRAPH}');"
        done
    fi
}

LOAD_SQL=$(build_load_sql)
FILE_COUNT=$(echo "$LOAD_SQL" | grep -c "TTLP" || true)
echo "$LOAD_SQL" | grep '^-- ' || true

# ---------------------------------------------------------------------------
# SQL execution via docker exec (one statement per line: isql rejects more)
# ---------------------------------------------------------------------------
echo "Running in ${MODE} mode..."

if [[ "$MODE" == "full" ]]; then
    docker exec -i "${CONTAINER}" /opt/virtuoso-opensource/bin/isql localhost:1111 dba "${DBA_PASSWORD}" <<EOF
log_enable(2);
RDF_GLOBAL_RESET();
DELETE FROM DB.DBA.SYS_XML_PERSISTENT_NS_DECL WHERE NS_PREFIX = 'go';
INSERT INTO DB.DBA.SYS_XML_PERSISTENT_NS_DECL (NS_PREFIX, NS_URL) VALUES ('go', 'http://purl.obolibrary.org/obo/GO_');

-- Prefix declarations
DB.DBA.XML_SET_NS_DECL ('dc', 'http://purl.org/dc/elements/1.1/', 2);
DB.DBA.XML_SET_NS_DECL ('dcterms', 'http://purl.org/dc/terms/', 2);
DB.DBA.XML_SET_NS_DECL ('rdfs', 'http://www.w3.org/2000/01/rdf-schema#', 2);
DB.DBA.XML_SET_NS_DECL ('foaf', 'http://xmlns.com/foaf/0.1/', 2);
DB.DBA.XML_SET_NS_DECL ('aop', 'https://identifiers.org/aop/', 2);
DB.DBA.XML_SET_NS_DECL ('aop.events', 'https://identifiers.org/aop.events/', 2);
DB.DBA.XML_SET_NS_DECL ('aop.relationships', 'https://identifiers.org/aop.relationships/', 2);
DB.DBA.XML_SET_NS_DECL ('aop.stressor', 'https://identifiers.org/aop.stressor/', 2);
DB.DBA.XML_SET_NS_DECL ('aopo', 'http://aopkb.org/aop_ontology#', 2);
DB.DBA.XML_SET_NS_DECL ('cas', 'https://identifiers.org/cas/', 2);
DB.DBA.XML_SET_NS_DECL ('inchikey', 'https://identifiers.org/inchikey/', 2);
DB.DBA.XML_SET_NS_DECL ('pato', 'http://purl.obolibrary.org/obo/PATO_', 2);
DB.DBA.XML_SET_NS_DECL ('ncbitaxon', 'http://purl.bioontology.org/ontology/NCBITAXON/', 2);
DB.DBA.XML_SET_NS_DECL ('cl', 'http://purl.obolibrary.org/obo/CL_', 2);
DB.DBA.XML_SET_NS_DECL ('uberon', 'http://purl.obolibrary.org/obo/UBERON_', 2);
DB.DBA.XML_SET_NS_DECL ('go', 'http://purl.obolibrary.org/obo/GO_', 2);
DB.DBA.XML_SET_NS_DECL ('mi', 'http://purl.obolibrary.org/obo/MI_', 2);
DB.DBA.XML_SET_NS_DECL ('mp', 'http://purl.obolibrary.org/obo/MP_', 2);
DB.DBA.XML_SET_NS_DECL ('hp', 'http://purl.obolibrary.org/obo/HP_', 2);
DB.DBA.XML_SET_NS_DECL ('pco', 'http://purl.obolibrary.org/obo/PCO_', 2);
DB.DBA.XML_SET_NS_DECL ('nbo', 'http://purl.obolibrary.org/obo/NBO_', 2);
DB.DBA.XML_SET_NS_DECL ('vt', 'http://purl.obolibrary.org/obo/VT_', 2);
DB.DBA.XML_SET_NS_DECL ('pr', 'http://purl.obolibrary.org/obo/PR_', 2);
DB.DBA.XML_SET_NS_DECL ('chebio', 'http://purl.obolibrary.org/obo/CHEBI_', 2);
DB.DBA.XML_SET_NS_DECL ('fma', 'http://purl.org/sig/ont/fma/fma', 2);
DB.DBA.XML_SET_NS_DECL ('cheminf', 'http://semanticscience.org/resource/CHEMINF_', 2);
DB.DBA.XML_SET_NS_DECL ('ncit', 'http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#', 2);
DB.DBA.XML_SET_NS_DECL ('comptox', 'https://comptox.epa.gov/dashboard/', 2);
DB.DBA.XML_SET_NS_DECL ('mmo', 'http://purl.obolibrary.org/obo/MMO_', 2);
DB.DBA.XML_SET_NS_DECL ('chebi', 'https://identifiers.org/chebi/', 2);
DB.DBA.XML_SET_NS_DECL ('chemspider', 'https://identifiers.org/chemspider/', 2);
DB.DBA.XML_SET_NS_DECL ('wikidata', 'https://identifiers.org/wikidata/', 2);
DB.DBA.XML_SET_NS_DECL ('chembl.compound', 'https://identifiers.org/chembl.compound/', 2);
DB.DBA.XML_SET_NS_DECL ('pubchem.compound', 'https://identifiers.org/pubchem.compound/', 2);
DB.DBA.XML_SET_NS_DECL ('drugbank', 'https://identifiers.org/drugbank/', 2);
DB.DBA.XML_SET_NS_DECL ('kegg.compound', 'https://identifiers.org/kegg.compound/', 2);
DB.DBA.XML_SET_NS_DECL ('lipidmaps', 'https://identifiers.org/lipidmaps/', 2);
DB.DBA.XML_SET_NS_DECL ('hmdb', 'https://identifiers.org/hmdb/', 2);
DB.DBA.XML_SET_NS_DECL ('ensembl', 'https://identifiers.org/ensembl/', 2);
DB.DBA.XML_SET_NS_DECL ('edam', 'http://edamontology.org/', 2);
DB.DBA.XML_SET_NS_DECL ('hgnc', 'https://identifiers.org/hgnc/', 2);
DB.DBA.XML_SET_NS_DECL ('ncbigene', 'https://identifiers.org/ncbigene/', 2);
DB.DBA.XML_SET_NS_DECL ('uniprot', 'https://identifiers.org/uniprot/', 2);
DB.DBA.XML_SET_NS_DECL ('void', 'http://rdfs.org/ns/void#', 2);
DB.DBA.XML_SET_NS_DECL ('pav', 'http://purl.org/pav/', 2);
DB.DBA.XML_SET_NS_DECL ('dcat', 'http://www.w3.org/ns/dcat#', 2);

-- SPARQL permissions
grant execute on "DB.DBA.EXEC_AS" to "SPARQL";
grant select on "DB.DBA.SPARQL_SINV_2" to "SPARQL";
grant execute on "DB.DBA.SPARQL_SINV_IMP" to "SPARQL";
grant SPARQL_LOAD_SERVICE_DATA to "SPARQL";
grant SPARQL_SPONGE to "SPARQL";

-- Load every quarter
${LOAD_SQL}
checkpoint;
exit;
EOF

else
    docker exec -i "${CONTAINER}" /opt/virtuoso-opensource/bin/isql localhost:1111 dba "${DBA_PASSWORD}" <<EOF
log_enable(2);
${LOAD_SQL}
checkpoint;
exit;
EOF

fi

# ---------------------------------------------------------------------------
# Completion message
# ---------------------------------------------------------------------------
echo "Mode: ${MODE}"
echo "Files loaded: ${FILE_COUNT}"
echo "Done."
