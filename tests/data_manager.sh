#!/bin/bash


# insert a number of rows (1 per second) into the test table, expected to have a single column (text)
# using sequential integers to so that it's easily predictable

ACTION=${1}
DB=${2:-pgcarpenter}
TABLE=${3:-data}
COLUMN=${4:-id}
N_ROWS=${5:-60}


for i in `seq 1 ${N_ROWS}`
do
    if [[ ${ACTION} = write ]]
    then
        psql -h localhost -U postgres -d ${DB} -c "INSERT INTO ${TABLE}(${COLUMN}) VALUES('${i}')"
        sleep 1
    else
        psql -h localhost -U postgres -d ${DB} -c "SELECT * FROM ${TABLE}" #\
        #"DO \$\$ BEGIN ASSERT (SELECT ${COLUMN} FROM ${TABLE} WHERE ${COLUMN}='${i}') = '${i}', 'not ${i}!';END;\$\$;"
    fi
done
