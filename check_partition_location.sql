--     When the location of the partition isn't based at the root of the table location, many assumptions
--     are no longer valid.
--
--     This script will help understand the impact.

use ${DB};

SELECT
    db_name,
    tbl_name,
    tbl_location,
    part_location,
    part_compliance
FROM (
select
    db_name,
    tbl_name,
    tbl_location,
    part_location,
    CASE
        WHEN instr(part_location,tbl_location) = 1 THEN "IN"
        ELSE "OUT"
        END part_compliance
from
     hms_dump_${ENV}
where
      part_name is not null) sub
WHERE
    sub.part_compliance = "OUT";