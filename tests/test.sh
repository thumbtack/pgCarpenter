#!/bin/bash

# defaults
PG_VERSION=11
AWS_PROFILE=default
DELETE_ALL=0
# constants used throughout the script
PGCARPENTER_BIN=pgCarpenter
CONTAINER_NAME=pgcarpenter # name of the container given by `docker run`
DB_NAME=pgcarpenter # DB used for running the tests
TABLE_NAME=data # table to use
COLUMN_NAME=username # column to read/write test data
TEST_ROWS=60 # number of rows to write/read
BACKUP_NAME=functional-test
PG_LOG=postgresql.log


usage() {
    echo "Usage: $0 -b <s3_bucket> [-p aws_profile] [-v pg_version] [-d]"
    echo -e "\nThe -d option should be used to delete the ${PGCARPENTER_BIN} related contents of the"
    echo "S3 bucket *instead* of running the tests"
    echo -e "\nDefaults:"
    echo "  aws_profile: ${AWS_PROFILE}"
    echo "  pg_version: ${PG_VERSION}"
}

log() {
    echo $*
}

parse_args() {
    # make sure arguments exist
    if [[ $# -lt 2 ]]
    then
        usage
        exit 1
    fi

    # parse args
    while getopts ":b:p:v:d" opt; do
      case ${opt} in
        b )
          S3_BUCKET=${OPTARG}
          ;;
        p )
          AWS_PROFILE=${OPTARG}
          ;;
        v )
          PG_VERSION=${OPTARG}
          ;;
        d )
          DELETE_ALL=1
          ;;
        \? )
          echo "Invalid option: $OPTARG" 1>&2
          usage
          exit 1
          ;;
        : )
          echo "Invalid option: $OPTARG requires an argument" 1>&2
          usage
          exit 1
          ;;
      esac
    done
    shift $((OPTIND -1))

}

# set some variables
setup_environment() {
    # basic things like the Docker image, test DB name, table, number of rows
    # with synthetic data, etc.
    log "Setting variables"
    DOCKER_IMAGE_PRIMARY=postgres_primary_pgcarpenter:${PG_VERSION}
    DOCKER_IMAGE_REPLICA=postgres_replica_pgcarpenter:${PG_VERSION}

    # grab AWS access keys from the existing profile
    # do nothing if the environment variables are already set
    log "Setting up AWS credentials"
    if [[ -z "${AWS_ACCESS_KEY_ID}" ]]
    then
        AWS_ACCESS_KEY_ID=$(aws --profile ${AWS_PROFILE} configure get aws_access_key_id)
    fi
    if [[ -z "${AWS_SECRET_ACCESS_KEY}" ]]
    then
        AWS_SECRET_ACCESS_KEY=$(aws --profile ${AWS_PROFILE} configure get aws_secret_access_key)
    fi
    if [[ -z "${AWS_SESSION_TOKEN}" ]]
    then
        AWS_SESSION_TOKEN=$(aws --profile ${AWS_PROFILE} configure get aws_session_token)
    fi
}

build_pgcarpenter() {
    log "Building ${PGCARPENTER_BIN}"
    pushd ..
    make
    popd
    cp ../${PGCARPENTER_BIN} .
}

build_docker_image_primary() {
    log "Building Docker image ${DOCKER_IMAGE_PRIMARY}"
    build_pgcarpenter
    docker build -t ${DOCKER_IMAGE_PRIMARY} . --build-arg VERSION="${PG_VERSION}" > docker_build_primary.log
}

build_docker_image_replica() {
    log "Building Docker image ${DOCKER_IMAGE_REPLICA}"
    build_pgcarpenter
    restore_backup_cmd="/${PGCARPENTER_BIN} restore-backup \
        --s3-bucket ${S3_BUCKET} --backup-name ${BACKUP_NAME} \
        --data-directory /var/lib/postgresql/data --modified-only --verbose"
    restore_wal_cmd="/${PGCARPENTER_BIN} restore-wal --s3-bucket ${S3_BUCKET} --wal-filename %f --wal-path %p --verbose"
    docker build -t ${DOCKER_IMAGE_REPLICA} . \
    --build-arg VERSION="${PG_VERSION}" \
    --build-arg restore_backup_cmd="${restore_backup_cmd}" \
    --build-arg restore_wal_cmd="${restore_wal_cmd}" > docker_build_replica.log
}

wait_for_pg() {
    while [[ 1 ]]
    do

        if psql -h localhost -U postgres -c 'SELECT 1' > /dev/null 2>&1
        then
            break
        fi
        log "Waiting for PostgreSQL to become ready to accept connections..."
        sleep 10
    done
}

run_container() {
    mode=$1
    log "Starting Docker container ${CONTAINER_NAME} as ${mode}"
    # export the AWS access keys
    # archive WAL every second so that we can predictably archive some segments
    # use the most recent build of pgCarpenter to archive WAL
    if [[ ${mode} = replica ]]
    then
        docker run --rm -d \
            -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
            -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
            -e AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}" \
            --name ${CONTAINER_NAME} -p 5432:5432 ${DOCKER_IMAGE_REPLICA} \
            -c 'client_min_messages=DEBUG1' -c 'log_min_messages=DEBUG1' \
            -c 'archive_mode=on' -c 'wal_level=logical' -c 'hot_standby=on' \
            -c 'archive_command=' > docker_run_replica.log
    else
        docker run --rm -d \
            -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
            -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
            -e AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}" \
            --name ${CONTAINER_NAME} -p 5432:5432 ${DOCKER_IMAGE_PRIMARY} \
            -c 'archive_mode=on' -c 'archive_timeout=1' -c 'wal_level=logical' -c 'hot_standby=on' \
            -c "archive_command=/${PGCARPENTER_BIN} archive-wal --verbose --s3-bucket ${S3_BUCKET} --wal-path %p" > docker_run_primary.log
    fi
    # saving the session logs is useful for debugging and checking for no errors on the WAL archiving process
    log "Collecting PG logs on ${PG_LOG}"
    docker logs ${CONTAINER_NAME} -f &> ${PG_LOG} &
    # save the PID so that we can then wait for the process to be done
    PG_LOGS_PID=$!
    # it takes a couple of seconds for PG to start
    wait_for_pg
    # on replicas we want the DB to be created as part of the restore process
    if [[ ${mode} = primary ]]
    then
        log "Creating DB ${DB_NAME}"
        psql -h localhost -U postgres -f db.sql > /dev/null
        log "Creating the schema on DB ${DB_NAME}"
        psql -h localhost -U postgres -f schema.sql -d ${DB_NAME} > /dev/null
    fi
}

stop_container() {
    log "Stopping Docker container ${CONTAINER_NAME}"
    docker stop --time 3 ${CONTAINER_NAME} > /dev/null
    log 'Waiting for 'docker logs' to terminate'
    wait ${PG_LOGS_PID}
}

list_backups() {
    expected_number=${1}
    log "Listing backups"
    docker exec ${CONTAINER_NAME} /${PGCARPENTER_BIN} list-backups \
        --s3-bucket ${S3_BUCKET} > list_backups.log
    n=$(wc -l list_backups.log | cut -f1 -d' ')
    n=$((n - 1))
    if [[ ${n} -ne ${expected_number} ]]
    then
        log "Error: expected ${expected_number} backups, found ${n}"
    fi
    if ! grep 'LATEST' list_backups.log > /dev/null
    then
        log 'No complete backups found'
    fi
}

write_test_data_bg() {
    log "Starting the data writer job in the background"
    data_manager.sh write ${DB_NAME} ${TABLE_NAME} ${COLUMN_NAME} ${TEST_ROWS} > /dev/null &
    DATA_WRITER_PID=$!
}

wait_for_test_data() {
    log 'Waiting for the data writer job to terminate'
    wait ${DATA_WRITER_PID}
}

confirm_test_data() {
    log 'Confirming that the data are present in the source DB'
    data_manager.sh read ${DB_NAME} ${TABLE_NAME} ${COLUMN_NAME} ${TEST_ROWS} > /dev/null
}

create_backup() {
    # allow for some data to be written before taking the backup (the test writes 1 row per second)
    let "wait = ${TEST_ROWS} / 4"
    log "Waiting ${wait} seconds before creating a backup"
    sleep ${wait}
    log "Creating backup ${BACKUP_NAME}"
    docker exec ${CONTAINER_NAME} /${PGCARPENTER_BIN} create-backup\
        --s3-bucket ${S3_BUCKET} --backup-name ${BACKUP_NAME} \
        --data-directory /var/lib/postgresql/data --checkpoint > create_backup.log
    # confirm the backup successfully finished
    if ! grep 'Backup successfully completed' create_backup.log > /dev/null
    then
        log 'create-backup failed'
    fi
    # also make sure WAL archiving is peachy
    if ! grep 'Finished archiving WAL segment' ${PG_LOG} > /dev/null
    then
        log 'WAL archiving failed'
    fi
}

delete_backup(){
    log "Deleting backup"
    docker exec ${CONTAINER_NAME} /${PGCARPENTER_BIN} delete-backup \
        --s3-bucket ${S3_BUCKET} --backup-name ${BACKUP_NAME} --verbose > delete_backup.log
    if ! grep 'Backup successfully deleted' delete_backup.log > /dev/null
    then
        log 'delete-backup failed'
    fi
}


parse_args $@
setup_environment

# trying to cleanup the S3 bucket there's
if [[ ${DELETE_ALL} -eq 1 ]]
then
    log "Deleting the contents of ${S3_BUCKET}"
    aws s3 rm --recursive s3://${S3_BUCKET}/${BACKUP_NAME}/
    aws s3 rm --recursive s3://${S3_BUCKET}/WAL/
    log "Done"
    exit
fi

log "Running tests on PG version ${PG_VERSION} using the ${S3_BUCKET} S3 bucket with the ${AWS_PROFILE} AWS profile"

# create a backup
log "== create-backup =="
build_docker_image_primary
run_container primary
write_test_data_bg
create_backup
wait_for_test_data
confirm_test_data
# give it some time to archive all WAL with test data (we're generating 1 segment per second)
sleep 3
stop_container

# list and ensure a backup exists and was successfully completed
log "== list-backup =="
# it's faster to launch a primary than a replica which restores a backup
build_docker_image_primary
run_container primary
list_backups 1
stop_container

# restore the backup
log "== restore-backup =="
build_docker_image_replica
run_container replica
# wait for the base backup to be restored
while [[ 1 ]]
do
    if grep 'Backup successfully restored' ${PG_LOG} > /dev/null
    then
        break
    fi
    log "Restoring the base backup..."
    sleep 15
done
# wait for PG to be available
while [[ 1 ]]
do
    if grep 'consistent recovery state reached' ${PG_LOG} > /dev/null
    then
        break
    fi
    log "Starting hot-standby..."
    sleep 15
done
# it might take a couple of seconds for the startup process to be finished
wait_for_pg
confirm_test_data
stop_container

# delete the backup
log "== delete-backup =="
build_docker_image_primary
run_container primary
delete_backup
stop_container

# list to make sure there are no backups
log "== list-backup =="
# it's faster to launch a primary than a replica which restores a backup
build_docker_image_primary
run_container primary
list_backups 0
stop_container

grep -i -E 'error|fail|fatal' ${PG_LOG}
