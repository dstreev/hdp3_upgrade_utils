USE ${DB};

-- Distinct List of Serdes
WITH ALL_SERDES         AS (
                           SELECT DISTINCT
                               sub.serde
                           FROM
                               (
                               SELECT DISTINCT tbl_serde_slib AS serde
                               FROM hms_dump_${ENV}
                               UNION ALL
                               SELECT part_serde_slib AS serde
                               FROM hms_dump_${ENV}
                               ) AS sub
                           )
   , TBL_WITH_SERDES    AS (
                           SELECT db_name, tbl_name, tbl_serde_slib, count(*)
                           FROM hms_dump_${ENV}
                           GROUP BY db_name, tbl_name, tbl_serde_slib
                           )
   , QUESTIONABLE_SERDE AS (
                           SELECT a.serde AS serde
                           FROM ALL_SERDES a
                           WHERE a.serde NOT IN ( SELECT serde_name FROM known_serdes_${ENV} )
                           )
SELECT DISTINCT
    db_name
  , tbl_name
  , tbl_serde_slib
FROM
    TBL_WITH_SERDES t
WHERE
        t.tbl_serde_slib IN ( SELECT a.serde FROM ALL_SERDES a WHERE a.serde IN ( SELECT * FROM QUESTIONABLE_SERDE ) );

