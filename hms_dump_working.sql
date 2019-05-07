/*

Scratch working queries

Variables:
DB - The database you placed the hms dump table.
ENV - IE: dev,qa,prod.  Used to support multiple 
    environment dump files in the same database.

*/

USE ${DB};

-- Distinct List of Serdes
SELECT DISTINCT
    tbl_serde_slib,
    part_serde_slib
FROM
    hms_dump_${ENV};

-- Find Table with serde.
SELECT
    db_name ,
    tbl_name,
    COUNT(*)
FROM
    hms_dump_${ENV}
WHERE
    tbl_serde_slib = "${SERDE}"
GROUP BY
    db_name,
    tbl_name;

SELECT
    db_name,
    tbl_name
FROM
    hms_dump_${ENV}
WHERE
    tbl_serde_slib IS NULL;
SELECT
    *
FROM
    hms_dump_${ENV}
WHERE
    db_name LIKE 'priv_%';


-- Find the ACID tables
SELECT DISTINCT
    db_name ,
    tbl_name,
    tbl_location
FROM
    hms_dump_${ENV}
WHERE
    tbl_param_key = 'transactional'
    AND tbl_param_value = 'true';

-- ACID table standard location status
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

-- Find Managed Tables that will be converted to ACID.
-- QUESTION: DOES THE LOCATION MATTER HERE: IE: Needs to be in the warehouse directory.
SELECT
    db_name ,
    tbl_name,
    tbl_type,
    tbl_serde_slib,
    regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)',2) AS hdfs_path,
    -- Look for Manage table that are NOT 'transaction' AND are ORC format AND are MANAGED.
    CASE
      WHEN !array_contains(collect_set(concat_ws(":",tbl_param_key,tbl_param_value)) ,
        "transactional:true")
        AND tbl_type = "MANAGED_TABLE"
        -- If the base directory is the warehouse, then it may be migrated if owned by 'hive'.
        AND instr(regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)',2),'/apps/hive/warehouse') = 1
        AND tbl_serde_slib = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
      THEN "ACIDv2/Migrate"
      WHEN !array_contains(collect_set(concat_ws(":",tbl_param_key,tbl_param_value)) ,
        "transactional:true")
        AND tbl_type = "MANAGED_TABLE"
        AND tbl_serde_slib = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
      THEN "ACIDv2"
      WHEN !array_contains(collect_set(concat_ws(":",tbl_param_key,tbl_param_value)) ,
        "transactional:true")
        AND tbl_type = "MANAGED_TABLE"
        -- If the base directory is the warehouse, then it may be migrated if owned by 'hive'.
        AND instr(regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)',2),'/apps/hive/warehouse') = 1
        AND tbl_serde_slib != "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
      THEN "ACIDv2(append)/Migrate"
      WHEN !array_contains(collect_set(concat_ws(":",tbl_param_key,tbl_param_value)) ,
        "transactional:true")
        AND tbl_type = "MANAGED_TABLE"
        AND tbl_serde_slib != "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
      THEN "ACIDv2(append)"
      ELSE "NO"
    END CONVERSION_POSSIBLE
FROM
    hms_dump_${ENV}
WHERE
    db_name != "information_schema"
    AND db_name != "sys"
    AND tbl_name is not null
GROUP BY
    db_name ,
    tbl_name,
    tbl_type,
    tbl_location,
    tbl_serde_slib
ORDER BY db_name, tbl_name;

-- List all distinct Tbl and Partition Directories
-- Used the resulting 'hdfs_path' items to validate existence.
SELECT
    db_name ,
    tbl_name,
    tbl_type,
    null as part_name,
    concat('test ', '-e ', regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)',2)) AS hdfs_path,
    count(1)
from
    hms_dump_${ENV}
where
    part_name is null
    and tbl_type != 'VIRTUAL_VIEW'
group by
    db_name,tbl_name,tbl_type, part_name,
    regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)',2)
union all
select
    db_name,
    tbl_name,
    tbl_type,
    part_name,
    concat('test ', '-e ', regexp_extract(part_location, 'hdfs://([^/]+)(.*)',2)) AS hdfs_path,
    count(1)
from
    hms_dump_${ENV}
where
    part_name is not null
    and tbl_type != 'VIRTUAL_VIEW'
group by
    db_name,tbl_name,tbl_type, part_name,
    regexp_extract(part_location, 'hdfs://([^/]+)(.*)',2);        
    
    
    
SELECT
    db_name ,
    tbl_name,
    collect_list(tbl_param_key)
FROM
    hms_dump_${ENV}
GROUP BY
    db_name,
    tbl_name;

