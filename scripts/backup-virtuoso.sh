#!/bin/sh
#
# Nightly hot backup of the multi-version Virtuoso (dashboard #162).
#
# Runs as the swarm cron job cronjobs_aopwiki-dashboard-backup, in the Virtuoso
# image itself (it has isql, tar and gzip), on any node:
#   1. backup_online() of the whole database into /database/backups/STAMP,
#      written by the server while it keeps answering queries (no downtime);
#   2. that folder plus virtuoso.ini packed into
#      /backup/virtuoso-STAMP.tar.gz on HiDrive, with a .sha256 and a
#      .manifest (graph and triple counts at backup time);
#   3. old backups pruned: KEEP_REMOTE on HiDrive, KEEP_LOCAL on GlusterFS.
#
# Mounts the job expects:
#   /database   the Virtuoso data dir (/mnt/gluster/docker/aopwiki-dashboard/virtuoso-data)
#   /backup     /mnt/hidrive/docker/aopwiki-dashboard/virtuoso (root-owned cifs:
#               the job runs as root, which the image does by default)
# and the swarm secret virtuoso_dba_password. Restore steps: DEPLOY.md.
#
set -eu

VIRTUOSO=${VIRTUOSO_HOST:-aopwiki-dashboard_virtuoso}
SECRET=/run/secrets/${SECRET_NAME:-virtuoso_dba_password}
DB=/database
OUT=/backup
KEEP_REMOTE=${KEEP_REMOTE:-14}
KEEP_LOCAL=${KEEP_LOCAL:-2}
PREFIX=aopwiki-
ISQL=/opt/virtuoso-opensource/bin/isql

STAMP=$(date -u +%Y%m%d-%H%M%S)
REL=backups/$STAMP            # relative to the server's working dir, /database
DIR=$DB/$REL
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

say() { echo "[$(date -u +%H:%M:%S)] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

[ -r "$SECRET" ] || die "secret $SECRET not mounted"
[ -d "$OUT" ] || die "$OUT not mounted"
[ -f "$DB/virtuoso.ini" ] || die "$DB is not the Virtuoso data dir"

isql() {   # sql file -> output; fails on any isql error
    "$ISQL" "$VIRTUOSO:1111" dba "$(cat "$SECRET")" < "$1" > "$TMP/isql.out" 2>&1 || true
    if grep -q '\*\*\* Error' "$TMP/isql.out"; then cat "$TMP/isql.out" >&2; die "isql reported an error"; fi
    cat "$TMP/isql.out"
}

# backup_online writes into DirsAllowed only; "." (= /database) covers backups/.
mkdir -p "$DIR"
say "Backing up $VIRTUOSO into $REL"
cat > "$TMP/backup.sql" <<EOF
backup_context_clear();
checkpoint;
backup_online('$PREFIX', 1000000, 0, vector('./$REL'));
EOF
isql "$TMP/backup.sql" > /dev/null
ls "$DIR"/"$PREFIX"*.bp > /dev/null 2>&1 || die "backup_online wrote no files into $DIR"

# What a restore should give back.
cat > "$TMP/count.sql" <<'EOF'
SPARQL SELECT ?g (COUNT(*) AS ?triples) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY ?g;
EOF
isql "$TMP/count.sql" | grep -E '^http' | awk '{print $1, $2}' > "$DIR/graphs.txt"
GRAPHS=$(wc -l < "$DIR/graphs.txt")
TRIPLES=$(awk '{n += $2} END {print n + 0}' "$DIR/graphs.txt")
[ "$GRAPHS" -gt 0 ] || die "could not count the graphs"
say "$GRAPHS graphs, $TRIPLES triples"

ARCHIVE=$OUT/virtuoso-$STAMP.tar.gz
say "Writing $(basename "$ARCHIVE")"
cp "$DB/virtuoso.ini" "$DIR/virtuoso.ini"
tar -czf "$TMP/archive.tar.gz" -C "$DIR" .
tar -tzf "$TMP/archive.tar.gz" > /dev/null || die "the archive does not read back"
cp "$TMP/archive.tar.gz" "$ARCHIVE.part"
mv "$ARCHIVE.part" "$ARCHIVE"
(cd "$OUT" && sha256sum "$(basename "$ARCHIVE")" > "$(basename "$ARCHIVE").sha256" && sha256sum -c --quiet "$(basename "$ARCHIVE").sha256") \
    || die "checksum mismatch on $ARCHIVE"
{
    echo "stamp: $STAMP"
    echo "virtuoso: $VIRTUOSO"
    echo "prefix: $PREFIX"
    echo "graphs: $GRAPHS"
    echo "triples: $TRIPLES"
    echo "size: $(du -h "$ARCHIVE" | cut -f1)"
} > "$OUT/virtuoso-$STAMP.manifest"

# Keep the newest KEEP_REMOTE archives and KEEP_LOCAL backup folders.
prune() {   # keep, then the candidates, oldest first
    keep=$1; shift
    n=$#
    for path in "$@"; do
        [ "$n" -le "$keep" ] && break
        say "Pruning $(basename "$path")"
        rm -rf "$path"
        case "$path" in *.tar.gz) rm -f "$path.sha256" "${path%.tar.gz}.manifest" ;; esac
        n=$((n - 1))
    done
}
# shellcheck disable=SC2046  # stamps contain no spaces
prune "$KEEP_REMOTE" $(ls -1d "$OUT"/virtuoso-*.tar.gz 2>/dev/null | sort)
# shellcheck disable=SC2046
prune "$KEEP_LOCAL" $(ls -1d "$DB"/backups/[0-9]*-[0-9]* 2>/dev/null | sort)

say "Done: $ARCHIVE ($(du -h "$ARCHIVE" | cut -f1))"
