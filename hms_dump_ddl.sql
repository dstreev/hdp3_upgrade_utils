--     Table Schema for the External HMS DUMP output from the
--     sqoop job.
--
--     Ensure the 'target-hdfs-dir' specified in the sqoop script matches
--     the default location for the table.
--
--     IE: target-hdfs-dir /warehouse/tablespace/external/hive/${DB}.db/hms_dump_${ENV}
--
--     This script should be run AFTER the sqoop job has run so that the sqoop job
--     can create the directory.
--
--     Variables:
--         DB - The database you placed the hms dump table.
--         ENV - IE: dev,qa,prod.  Used to support multiple
--                 environment dump files in the same database.

CREATE DATABASE IF NOT EXISTS ${DB};

USE ${DB};

DROP TABLE hms_dump_${ENV};

CREATE EXTERNAL TABLE hms_dump_${ENV} (
    DB_NAME            STRING,
    DB_DEFAULT_LOC     STRING,
    DB_OWNER           STRING,
    TBL_ID             STRING,
    TBL_NAME           STRING,
    TBL_OWNER          STRING,
    TBL_TYPE           STRING,
    TBL_INPUT_FORMAT   STRING,
    TBL_OUTPUT_FORMAT  STRING,
    TBL_LOCATION       STRING,
    TBL_NUM_BUCKETS    STRING,
    TBL_SERDE_SLIB     STRING,
    TBL_PARAM_KEY      STRING,
    TBL_PARAM_VALUE    STRING,
    PART_ID            STRING,
    PART_NAME          STRING,
    PART_INPUT_FORMAT  STRING,
    PART_OUTPUT_FORMAT STRING,
    PART_LOCATION      STRING,
    PART_NUM_BUCKETS   STRING,
    PART_SERDE_SLIB    STRING
) ROW FORMAT DELIMITED NULL DEFINED AS '\002' STORED AS TEXTFILE LOCATION '${EXTERNAL_WAREHOUSE_DIR}/${DB}.db/hms_dump_${ENV}' TBLPROPERTIES ( "external.table.purge" = "true" );

DROP TABLE dir_size_${ENV};
CREATE EXTERNAL TABLE dir_size_${ENV} (
    num_of_folders INT, num_of_files INT, size BIGINT, directory STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '${EXTERNAL_WAREHOUSE_DIR}/${DB}.db/dir_size_${ENV}' TBLPROPERTIES ( "external.table.purge" = "true" );

DROP TABLE paths_${ENV};
CREATE EXTERNAL TABLE paths_${ENV} (
    path STRING
) PARTITIONED BY (section STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '${EXTERNAL_WAREHOUSE_DIR}/${DB}.db/paths_${ENV}' TBLPROPERTIES ( "external.table.purge" = "true" );

-- Add static partition to store managed table directories where we found delta records
ALTER TABLE paths_${ENV}
    ADD PARTITION (section = "managed_deltas");

