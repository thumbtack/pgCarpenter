#!/usr/bin/env bash

# restore the test base backup
if [[ ! -z ${RESTORE_BACKUP_CMD} ]]
then
    # https://hub.docker.com/_/postgres
    #
    # the initialization scripts such as this one, placed under /docker-entrypoint-initdb.d, are actually
    # executed *while PG is running*; the caller script starts PG, runs everything in the directory as per
    # the docs, and then stops PG
    #
    # given that the purpose of this script is to restore a base backup, PG cannot be running

    # stop PG
    echo "pgCarpenter: stopping PostgreSQL"
    PGUSER="${PGUSER:-$POSTGRES_USER}" pg_ctl -D "$PGDATA" -m fast -w stop

    # remove the data directory as created by initdb (we want only the backup data available)
    echo "pgCarpenter:  destroying existing data directory"
    rm -fr /var/lib/postgresql/data/*

    # restore the base backup
    echo "${RESTORE_BACKUP_CMD}"
    eval "${RESTORE_BACKUP_CMD}"

    # set up recovery.conf
    #
    # this needs to be in place *before* starting PG bellow, otherwise it will fail with
    # `FATAL:  could not locate required checkpoint record`
    # as it'll have no way of finding the required WAL segment
    if [[ ! -z ${RESTORE_WAL_CMD} ]]
    then
        cat << EOF > /var/lib/postgresql/data/recovery.conf
        standby_mode = 'on'
        restore_command = '${RESTORE_WAL_CMD}'
        recovery_target_timeline = 'latest'
EOF
    fi

    # start PG
    ls -la /var/lib/postgresql/data/
    echo "pgCarpenter: starting PostgreSQL"
    PGUSER="${PGUSER:-$POSTGRES_USER}" pg_ctl -D "$PGDATA" -w start
fi
