-- ACID Table Details
--
-- Variables:
-- DB - The database you placed the hms dump table.
-- ENV - IE: dev,qa,prod.  Used to support multiple
-- environment dump files in the same database.
USE ${DB};

-- List ACID Tables.
SELECT DISTINCT
    DB_NAME
  , TBL_NAME
  , TBL_LOCATION
FROM
    HMS_DUMP_${ENV}
WHERE
      TBL_PARAM_KEY = 'transactional'
  AND TBL_PARAM_VALUE = 'true';
