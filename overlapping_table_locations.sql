USE ${DB};

WITH D_TBL_LOCATIONS AS (
                        SELECT DISTINCT
                            db_name
                          , tbl_name
                          , tbl_type
                          , part_name
                          , CASE
                                WHEN PART_NAME IS NULL
                                    THEN regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2)
                                WHEN PART_NAME IS NOT NULL
                                    THEN regexp_extract(part_location, 'hdfs://([^/]+)(.*)', 2)
                            END AS tbl_location
                        FROM
                            hms_dump_${ENV}
                        WHERE
                              db_name != 'sys'
                          AND db_name != 'information_schema'
                        )
SELECT
    tbl_location
  , SIZE(COLLECT_SET(
        CONCAT(db_name, ".", tbl_name, "[Partition:", NVL(part_name, "DEFAULT"), "]")))                             AS TBL_PARTS_SHARING_LOCATION
  , COLLECT_SET(CONCAT(db_name, ".", tbl_name, "[Partition:", NVL(part_name, "DEFAULT"), "]", ":(", tbl_type,
                       ")"))                                                                                        AS DB_TBLS
FROM
    D_TBL_LOCATIONS
WHERE
      db_name != 'sys'
  AND db_name != 'information_schema'
GROUP BY tbl_location
HAVING
        SIZE(COLLECT_SET(
                CONCAT(db_name, ".", tbl_name, "[Partition:", NVL(part_name, "DEFAULT"), "]", ":(", tbl_type, ")"))) > 1;