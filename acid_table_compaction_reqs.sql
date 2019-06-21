-- ACID Table Details
--
-- Variables:
-- DB - The database you placed the hms dump table.
-- ENV - IE: dev,qa,prod.  Used to support multiple
-- environment dump files in the same database.
USE ${DB};

-- List ACID Tables.
WITH directories AS (
                    SELECT DISTINCT
                        DB_NAME
                      , TBL_NAME
                      , PART_NAME
                      , CASE
                            WHEN PART_NAME IS NULL
                                THEN regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2)
                            WHEN PART_NAME IS NOT NULL
                                THEN regexp_extract(part_location, 'hdfs://([^/]+)(.*)', 2)
                        END AS LOCATION
                    FROM
                        HMS_DUMP_${ENV}
                    WHERE
                          TBL_PARAM_KEY = 'transactional'
                      AND TBL_PARAM_VALUE = 'true'
                    )
SELECT
    --     COUNT(1)
    CONCAT("ALTER TABLE ", DB_NAME, ".", TBL_NAME, CASE
                                                       WHEN PART_NAME IS NOT NULL
                                                           THEN CONCAT(" PARTITION (", part_name, ")")
                                                       ELSE ""
                                                   END, " COMPACT 'MAJOR';") AS SQL
FROM
    directories d INNER JOIN paths_${ENV} p
                             ON d.LOCATION = p.path
WHERE
    p.section = "managed_deltas";

