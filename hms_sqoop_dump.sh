#!/usr/bin/env bash

# Used to extract a dataset out of the Metastore DB.
while [ $# -gt 0 ]; do
  case "$1" in
    --target-hdfs-dir)
      shift
      TARGET_HDFS_DIR=$1;
      shift
      ;;
    --jdbc-db-url)
      shift
      JDBC_DB_URL=$1
      shift
      ;;
    --jdbc-user)
      shift
      JDBC_USER=$1
      shift
      ;;
    --jdbc-password)
      shift
      JDBC_PASSWORD=$1
      shift
      ;;
    --lower)
      shift
      LOWER='TRUE'
      ;;
    *)
      break;
  esac
done

if [ "${TARGET_HDFS_DIRx}" == "x" ]; then
    echo "Missing --target-hdfs-dir"
    exit -1
fi
if [ "${JDBC_DB_URLx}" == "x" ]; then
    echo "Missing --jdbc-db-url"
    exit -1
fi
if [ "${JDBC_USERx}" == "x" ]; then
    echo "Missing --jdbc-user"
    exit -1
fi
if [ "${JDBC_PASSWORDx}" == "x" ]; then
    echo "Missing --jdbc-password"
    exit -1
fi

# This situation 'may' happen if the original MySql/MariaDB conf file for the source DB
# has 'lower_case_table_names' set and the restored version does NOT.
if [ "${LOWERx}" == "x" ]; then
    SELECT = "SELECT D.NAME as DB_NAME , D.DB_LOCATION_URI as DB_DEFAULT_LOC , D.OWNER_NAME as DB_OWNER , T.TBL_ID as TBL_ID , T.TBL_NAME as TBL_NAME , T.OWNER as TBL_OWNER , T.TBL_TYPE as TBL_TYPE , S.INPUT_FORMAT as TBL_INPUT_FORMAT , S.OUTPUT_FORMAT as TBL_OUTPUT_FORMAT , S.LOCATION as TBL_LOCATION , S.NUM_BUCKETS as TBL_NUM_BUCKETS , SER.SLIB as TBL_SERDE_SLIB , PARAMS.PARAM_KEY as TBL_PARAM_KEY , PARAMS.PARAM_VALUE as TBL_PARAM_VALUE , P.PART_ID as PART_ID , P.PART_NAME as PART_NAME , PS.INPUT_FORMAT as PART_INPUT_FORMAT , PS.OUTPUT_FORMAT as PART_OUTPUT_FORMAT, PS.LOCATION as PART_LOCATION , PS.NUM_BUCKETS as PART_NUM_BUCKETS , PSER.SLIB as PART_SERDE_SLIB FROM DBS D INNER JOIN TBLS T ON D.DB_ID = T.DB_ID LEFT OUTER JOIN SDS S ON T.SD_ID = S.SD_ID LEFT OUTER JOIN SERDES SER ON S.SERDE_ID = SER.SERDE_ID LEFT OUTER JOIN TABLE_PARAMS ON T.TBL_ID = PARAMS.TBL_ID LEFT OUTER JOIN PARTITIONS P ON T.TBL_ID = P.TBL_ID LEFT OUTER JOIN SDS PS ON P.SD_ID = PS.SD_ID LEFT OUTER JOIN SERDES PSER ON PS.SERDE_ID = PSER.SERDE_ID WHERE $CONDITIONS;"
else
    SELECT = "SELECT D.NAME as DB_NAME , D.DB_LOCATION_URI as DB_DEFAULT_LOC , D.OWNER_NAME as DB_OWNER , T.TBL_ID as TBL_ID , T.TBL_NAME as TBL_NAME , T.OWNER as TBL_OWNER , T.TBL_TYPE as TBL_TYPE , S.INPUT_FORMAT as TBL_INPUT_FORMAT , S.OUTPUT_FORMAT as TBL_OUTPUT_FORMAT , S.LOCATION as TBL_LOCATION , S.NUM_BUCKETS as TBL_NUM_BUCKETS , SER.SLIB as TBL_SERDE_SLIB , PARAMS.PARAM_KEY as TBL_PARAM_KEY , PARAMS.PARAM_VALUE as TBL_PARAM_VALUE , P.PART_ID as PART_ID , P.PART_NAME as PART_NAME , PS.INPUT_FORMAT as PART_INPUT_FORMAT , PS.OUTPUT_FORMAT as PART_OUTPUT_FORMAT, PS.LOCATION as PART_LOCATION , PS.NUM_BUCKETS as PART_NUM_BUCKETS , PSER.SLIB as PART_SERDE_SLIB FROM dbs D INNER JOIN tbls T ON D.DB_ID = T.DB_ID LEFT OUTER JOIN sds S ON T.SD_ID = S.SD_ID LEFT OUTER JOIN serdes SER ON S.SERDE_ID = SER.SERDE_ID LEFT OUTER JOIN table_params PARAMS ON T.TBL_ID = PARAMS.TBL_ID LEFT OUTER JOIN partitions P ON T.TBL_ID = P.TBL_ID LEFT OUTER JOIN sds PS ON P.SD_ID = PS.SD_ID LEFT OUTER JOIN serdes PSER ON PS.SERDE_ID = PSER.SERDE_ID WHERE $CONDITIONS;"
fi

sqoop-import \
--query ${SELECT} \
--target-dir ${TARGET_HDFS_DIR} \
-m 1 \
--connect ${JDBC_DB_URL} \
--username ${JDBC_USER} \
--password ${JDBC_PASSWORD} \
--fields-terminated-by "\001" \
--null-string '\002'

