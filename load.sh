#!/bin/bash
set -euo pipefail
#
# Virtuoso Loader Script (multi-graph, versioned)
# Usage: ./load.sh [--full|--incremental] [--yes] [--help]
#
#   --full          Full reload: deletes all data, re-registers all files.
#                   Requires confirmation unless --yes is also provided.
#   --incremental   (default) Register files; Virtuoso skips already-loaded ones.
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
  --full          Full reload (deletes all data, re-registers all files).
                  Prompts for confirmation unless --yes is also given.
  --incremental   Register files, skip already-loaded ones (default).
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
# build_load_funcs()
# Scans aopwikirdf/ for versioned TTL files and generates ld_dir() SQL calls.
# Paths reference /database/data (container-side volume mount point).
# ---------------------------------------------------------------------------
build_load_funcs() {
    local container_data_dir="/database/data"
    for file in "${SCRIPT_DIR}/aopwikirdf/AOPWikiRDF-"*.ttl \
                "${SCRIPT_DIR}/aopwikirdf/AOPWikiRDF-Genes-"*.ttl \
                "${SCRIPT_DIR}/aopwikirdf/AOPWikiRDF-Void-"*.ttl; do
        if [[ -f "$file" ]]; then
            local base
            base=$(basename "$file")
            local version
            version=$(echo "$base" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}')
            [[ -z "$version" ]] && continue
            echo "ld_dir('${container_data_dir}', '${base}', 'http://aopwiki.org/graph/${version}');"
        fi
    done
}

# Capture ld_dir() calls on the host before sending to docker exec
LOAD_CMDS=$(build_load_funcs)
FILE_COUNT=$(echo "$LOAD_CMDS" | grep -c "ld_dir" || true)

# ---------------------------------------------------------------------------
# SQL execution via docker exec
# ---------------------------------------------------------------------------
echo "Running in ${MODE} mode..."

if [[ "$MODE" == "full" ]]; then
    docker exec -i "${CONTAINER}" /opt/virtuoso-opensource/bin/isql localhost:1111 dba "${DBA_PASSWORD}" <<EOF
log_enable(2);
RDF_GLOBAL_RESET();
DELETE FROM load_list WHERE ll_graph LIKE 'http://aopwiki.org/graph/%';
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

-- Dynamically register TTL files
${LOAD_CMDS}

rdf_loader_run();
checkpoint;
SELECT DISTINCT ll_graph FROM DB.DBA.load_list;
exit;
EOF

else
    # incremental mode
    docker exec -i "${CONTAINER}" /opt/virtuoso-opensource/bin/isql localhost:1111 dba "${DBA_PASSWORD}" <<EOF
log_enable(2);

-- Dynamically register TTL files (already-loaded files are skipped via ll_state=2)
${LOAD_CMDS}

rdf_loader_run();
checkpoint;
SELECT DISTINCT ll_graph FROM DB.DBA.load_list WHERE ll_state = 2;
exit;
EOF

fi

# ---------------------------------------------------------------------------
# Completion message
# ---------------------------------------------------------------------------
echo "Mode: ${MODE}"
echo "Files registered: ${FILE_COUNT}"
echo "Done."
