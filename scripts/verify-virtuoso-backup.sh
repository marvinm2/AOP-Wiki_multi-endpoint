#!/bin/sh
#
# Restore a backup made by backup-virtuoso.sh into a throwaway Virtuoso and
# check that every graph comes back with the triple count in its manifest
# (dashboard #162). Touches nothing live: the restore happens inside this
# container's own filesystem and is gone when it exits.
#
#   sh verify-virtuoso-backup.sh [ARCHIVE]     (default: the newest in /backup)
#
# Runs in the Virtuoso image with /backup (HiDrive, read-only is enough) and the
# swarm secret virtuoso_dba_password, which the restored database keeps.
#
set -eu

SECRET=/run/secrets/${SECRET_NAME:-virtuoso_dba_password}
BIN=/opt/virtuoso-opensource/bin
PREFIX=aopwiki-
ARCHIVE=${1:-$(ls -1 /backup/virtuoso-*.tar.gz 2>/dev/null | sort | tail -1)}
W=/database   # DatabaseFile in the backed-up virtuoso.ini is ../database/virtuoso.db

say() { echo "[$(date -u +%H:%M:%S)] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

[ -n "$ARCHIVE" ] && [ -f "$ARCHIVE" ] || die "no archive found"
[ -r "$SECRET" ] || die "secret $SECRET not mounted"
[ -e "$W/virtuoso.db" ] && die "$W already holds a database; run this in a fresh container"

say "Checking $(basename "$ARCHIVE")"
(cd "$(dirname "$ARCHIVE")" && sha256sum -c --quiet "$(basename "$ARCHIVE").sha256") || die "checksum mismatch"
mkdir -p "$W"
tar -xzf "$ARCHIVE" -C "$W"
cd "$W"

say "Restoring"
"$BIN/virtuoso-t" +configfile virtuoso.ini +restore-backup "$PREFIX" > restore.log 2>&1 \
    || { tail -20 restore.log >&2; die "restore failed"; }
[ -s virtuoso.db ] || die "restore produced no virtuoso.db"

say "Starting the restored database"
"$BIN/virtuoso-t" +configfile virtuoso.ini +foreground > server.log 2>&1 &
PASS=$(cat "$SECRET")
up=false
for _ in $(seq 1 60); do
    if echo "status();" | "$BIN/isql" localhost:1111 dba "$PASS" > /dev/null 2>&1; then up=true; break; fi
    sleep 2
done
$up || { tail -20 server.log >&2; die "the restored database did not start"; }

echo "SPARQL SELECT ?g (COUNT(*) AS ?triples) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY ?g;" \
    | "$BIN/isql" localhost:1111 dba "$PASS" | grep -E '^http' | awk '{print $1, $2}' > restored.txt
echo "shutdown();" | "$BIN/isql" localhost:1111 dba "$PASS" > /dev/null 2>&1 || true

if ! diff graphs.txt restored.txt > graphs.diff; then
    cat graphs.diff >&2
    die "the restored graphs differ from the manifest"
fi
say "OK: $(wc -l < restored.txt) graphs, $(awk '{n += $2} END {print n + 0}' restored.txt) triples, as backed up"
