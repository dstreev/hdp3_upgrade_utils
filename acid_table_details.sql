--   ACID Table Details
--
--       Variables:
--         DB - The database you placed the hms dump table.
--         ENV - IE: dev,qa,prod.  Used to support multiple
--                 environment dump files in the same database.


USE ${DB};

-- List ACID Tables.
SELECT DISTINCT
    db_name ,
    tbl_name,
    tbl_location
FROM
    hms_dump_${ENV}
WHERE
    tbl_param_key = 'transactional'
    AND tbl_param_value = 'true';

