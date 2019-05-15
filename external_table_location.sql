/*

 */

USE ${DB};

SELECT DISTINCT
    db_name,
    tbl_name,
    regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)',2) tbl_location
FROM
    hms_dump_${ENV}
WHERE
    tbl_type = "EXTERNAL_TABLE";