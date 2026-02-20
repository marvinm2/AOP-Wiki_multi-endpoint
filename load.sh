#!/bin/bash
#
# Virtuoso Loader Script (multi-graph, versioned)
# Usage: ./load.sh [log_file] [virtuoso_password]

args=("$@")

if [ $# -ne 2 ]; then
    echo "Wrong number of arguments. Correct usage: \"load.sh [log_file] [virtuoso_password]\""
    exit 1
fi

LOGFILE=${args[0]}
VIRT_PSWD=${args[1]}
VAD=data

# Build dynamic loading functions for all versioned files
build_load_funcs() {
    for file in $VAD/AOPWikiRDF-*.ttl $VAD/AOPWikiRDF-Genes-*.ttl $VAD/AOPWikiRDF-Void-*.ttl; do
        if [[ -f "$file" ]]; then
            base=$(basename "$file")
            version=$(echo "$base" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}')
            [[ -z "$version" ]] && continue
            echo "ld_dir('$VAD', '$base', 'http://aopwiki.org/graph/$version');"
        fi
    done
}

isql_cmd="isql -U dba -P $VIRT_PSWD"
isql_cmd_check="isql -U dba -P $VIRT_PSWD exec=\"checkpoint;\""

echo "Loading triples into versioned graphs..." > "$LOGFILE"

${isql_cmd} <<EOF &>> "$LOGFILE"
log_enable(2);
RDF_GLOBAL_RESET();

-- Clean up graphs (optional)
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

-- Dynamically insert ld_dir calls
$(build_load_funcs)

-- Load files
rdf_loader_run();
checkpoint;

-- Show load list
SELECT DISTINCT ll_graph FROM DB.DBA.load_list;

exit;
EOF

# Final status
echo "----------" >> "$LOGFILE"
echo "[✓] Done. Loaded all versioned TTL files. Check: $LOGFILE"
cat "$LOGFILE"
