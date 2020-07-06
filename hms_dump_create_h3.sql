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

CREATE EXTERNAL TABLE IF NOT EXISTS hms_dump_${ENV} (
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
)
    ROW FORMAT SERDE
        'org.apache.hive.storage.jdbc.JdbcSerDe'
    STORED BY
        'org.apache.hive.storage.jdbc.JdbcStorageHandler'
        WITH SERDEPROPERTIES (
        'serialization.format'='1')
    TBLPROPERTIES (
        'bucketing_version'='2',
        'hive.sql.database.type'='METASTORE',
        'hive.sql.query'='SELECT D.NAME as DB_NAME , D.DB_LOCATION_URI as DB_DEFAULT_LOC , D.OWNER_NAME as DB_OWNER , T.TBL_ID as TBL_ID , T.TBL_NAME as TBL_NAME , T.OWNER as TBL_OWNER , T.TBL_TYPE as TBL_TYPE , S.INPUT_FORMAT as TBL_INPUT_FORMAT , S.OUTPUT_FORMAT as TBL_OUTPUT_FORMAT , S.LOCATION as TBL_LOCATION , S.NUM_BUCKETS as TBL_NUM_BUCKETS , SER.SLIB as TBL_SERDE_SLIB , PARAMS.PARAM_KEY as TBL_PARAM_KEY , PARAMS.PARAM_VALUE as TBL_PARAM_VALUE , P.PART_ID as PART_ID , P.PART_NAME as PART_NAME , PS.INPUT_FORMAT as PART_INPUT_FORMAT , PS.OUTPUT_FORMAT as PART_OUTPUT_FORMAT, PS.LOCATION as PART_LOCATION , PS.NUM_BUCKETS as PART_NUM_BUCKETS , PSER.SLIB as PART_SERDE_SLIB FROM DBS D INNER JOIN TBLS T ON D.DB_ID = T.DB_ID LEFT OUTER JOIN SDS S ON T.SD_ID = S.SD_ID LEFT OUTER JOIN SERDES SER ON S.SERDE_ID = SER.SERDE_ID LEFT OUTER JOIN TABLE_PARAMS PARAMS ON T.TBL_ID = PARAMS.TBL_ID LEFT OUTER JOIN PARTITIONS P ON T.TBL_ID = P.TBL_ID LEFT OUTER JOIN SDS PS ON P.SD_ID = PS.SD_ID LEFT OUTER JOIN SERDES PSER ON PS.SERDE_ID = PSER.SERDE_ID');

CREATE EXTERNAL TABLE IF NOT EXISTS dir_size_${ENV} (
    num_of_folders INT, num_of_files INT, size BIGINT, directory STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '${EXTERNAL_WAREHOUSE_DIR}/${DB}.db/dir_size_${ENV}' TBLPROPERTIES ( "external.table.purge" = "true" );

CREATE EXTERNAL TABLE IF NOT EXISTS paths_${ENV} (
    path STRING
) PARTITIONED BY (section STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '${EXTERNAL_WAREHOUSE_DIR}/${DB}.db/paths_${ENV}' TBLPROPERTIES ( "external.table.purge" = "true" );

-- Add static partition to store managed table directories where we found delta records
ALTER TABLE paths_${ENV}
    ADD IF NOT EXISTS PARTITION (section = "managed_deltas");

CREATE TABLE IF NOT EXISTS known_serdes_${ENV} (
    serde_name STRING
);

INSERT INTO TABLE
    known_serdes_${ENV} (serde_name)
VALUES ("org.apache.hadoop.hive.ql.io.orc.OrcSerde")
     , ("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
     , ("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
     , ("org.apache.hadoop.hive.hbase.HBaseSerDe")
     , ("org.apache.hive.storage.jdbc.JdbcSerDe")
     , ("org.apache.hadoop.hive.druid.DruidStorageHandler")
     , ("org.apache.phoenix.hive.PhoenixStorageHandler")
     , ("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
     , ("org.apache.hadoop.hive.serde2.RegexSerDe")
     , ("parquet.hive.serde.ParquetHiveSerDe")
     , ("org.apache.hadoop.hive.serde2.OpenCSVSerde")
     , ("org.apache.hive.hcatalog.data.JsonSerDe");