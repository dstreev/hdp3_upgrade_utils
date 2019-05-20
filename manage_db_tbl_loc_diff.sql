--     What 'managed' tables are NOT under the 'database' default location.
--
--     This could affect permissions and security around these tables.
--
--       Variables:
--         DB - The database you placed the hms dump table.
--         ENV - IE: dev,qa,prod.  Used to support multiple
--                 environment dump files in the same database.

USE ${DB};

SELECT DISTINCT DB_NAME,
                DB_DEFAULT_LOC,
                TBL_NAME,
                TBL_LOCATION
FROM hms_dump_${ENV}
WHERE tbl_type = "MANAGED_TABLE"
  AND db_name != 'sys'
  AND db_name != 'information_schema'
  AND instr(TBL_LOCATION, DB_DEFAULT_LOC) != 1;