--     Find Distinct Serde's used in the system.
--
--     If the serde isn't a common serde and no longer available
--     then the table schema needs to be removed.
--
--     Missing Serde's will cause the 'hive migration' script to fail.
--
--     Variables:
--         DB - The database you placed the hms dump table.
--         ENV - IE: dev,qa,prod.  Used to support multiple
--                 environment dump files in the same database.


USE ${DB};

-- Distinct List of Serdes
SELECT DISTINCT
    serde
FROM
    (
    SELECT DISTINCT tbl_serde_slib AS SERDE
    FROM hms_dump_${ENV}
    UNION ALL
    SELECT part_serde_slib AS SERDE
    FROM hms_dump_${ENV}
    ) AS sub;
