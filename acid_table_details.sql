/*
  ACID Table Details
  
      Variables:
        DB - The database you placed the hms dump table.
        ENV - IE: dev,qa,prod.  Used to support multiple 
                environment dump files in the same database.

*/

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


/*
   ACID table standard location status

   These tables will need to be compacted before upgrade.

   This can be done using the 'pre-upgrade' script and running it a few
   times before the upgrade window.  By running it early, you'll minimize
   the time spend during to upgrade process.
   
   The volume list here will give you an idea 'how long' the pre-upgrade
   script may take.
   
   If the list is long, ensure you've tuned the compactor with enough
   resources to address the volume requirements!!!
*/
SELECT DISTINCT
    db_name                                                     ,
    tbl_name                                                    ,
    regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)',2) AS path,
    CASE
      WHEN locate('/warehouse/tablespace/managed/hive', tbl_location) + locate
        ('/apps/hive/warehouse',tbl_location) > 0
      THEN 'YES'
      ELSE 'NO'
    END AS Standard_Location
FROM
    hms_dump_${ENV}
WHERE
    tbl_param_key = 'transactional'
    AND tbl_param_value = 'true'
    AND locate('/warehouse/tablespace/managed/hive', tbl_location) + locate('/apps/hive/warehouse',
    tbl_location) = 0;