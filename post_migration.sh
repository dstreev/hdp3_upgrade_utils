#!/usr/bin/env bash

# To trigger dryrun, just add an argument when calling this script.
DRYRUN=$1
HIVE_CMD=hive

if [[ "${DRYRUN}x" == "x" ]]; then
    echo "Dryrun!!!"
    DO_DRYRUN="--dryRun"
else
    DO_DRYRUN=""
fi

while read line; do
    POST_DB=`echo ${line} | cut -f 1 -d " "`
    echo "Launching Post Migration for: ${POST_DB}"
    nohup ${HIVE_CMD} -Dhive.log.dir=${OUTPUT_DIR} -Dhive.log.file=post_migration_output_${POST_DB}.log \
    --config /etc/hive/conf --service strictmanagedmigration --hiveconf hive.strict.managed.tables=true  \
    -m automatic  --dbRegex ${POST_DB} ${DO_DRYRUN} \
    --modifyManagedTables --oldWarehouseRoot /apps/hive/warehouse &
done < ${OUTPUT_DIR}/post_migration.txt
