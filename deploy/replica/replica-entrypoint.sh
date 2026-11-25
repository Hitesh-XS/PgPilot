#!/bin/bash
set -e
PGDATA=/var/lib/postgresql/data

if [ -z "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
  until pg_isready -h pg-primary -p 5432 -U postgres; do sleep 1; done
  gosu postgres bash -c "PGPASSWORD='$REPL_PASSWORD' pg_basebackup -h pg-primary -U replicator -D $PGDATA -R -X stream"
fi

chown -R postgres:postgres "$PGDATA"
chmod 700 "$PGDATA"
exec gosu postgres postgres -c hot_standby=on -c hot_standby_feedback=on $EXTRA_ARGS