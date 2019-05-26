ARG VERSION
FROM postgres:$VERSION

# restoring a base backup needs to happen with PG stopped, in our case that
# means before starting PG which implies executing the command from the init script
ARG restore_backup_cmd
# similarly, recovery.conf needs to be put in place before PG is started
ARG restore_wal_cmd

RUN DEBIAN_FRONTEND=noninteractive apt update && apt upgrade -y && apt install -y ca-certificates
COPY ./pgCarpenter /
RUN chmod 755 /pgCarpenter

# pass the arguments as environment variables so that the init script can use them
ENV RESTORE_BACKUP_CMD=$restore_backup_cmd
ENV RESTORE_WAL_CMD=$restore_wal_cmd
COPY ./entrypoint_restore_backup_and_recovery.conf.sh /docker-entrypoint-initdb.d/
RUN chmod 755 /docker-entrypoint-initdb.d/entrypoint_restore_backup_and_recovery.conf.sh
