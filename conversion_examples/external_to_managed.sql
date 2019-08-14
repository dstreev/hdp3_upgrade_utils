/*

    Example of how to create an external table and migrate it to
    a managed table.

*/

create database ${DB};

use ${DB};

drop table test_init_ext;

create external table test_init_ext (
    id string,
    street string)
STORED AS ORC
TBLPROPERTIES (
  'external.table.purge'='true'
);

-- Not a good practice approach for add records to Hive!!!
-- For demo purposes ONLY.  Don't do this in production systems!!!
insert into table test_init_ext (id, street) values ("1", "Hollywood");
insert into table test_init_ext (id, street) values ("2", "Hollywood");

insert into table test_init_ext_2 select * from test_init_ext;

select * from test_init_ext;

-- Change to Managed
ALTER TABLE test_init_ext set TBLPROPERTIES ('EXTERNAL'='false','transactional'='true');

-- This will fail.
ALTER TABLE test_init_ext set TBLPROPERTIES ('EXTERNAL'='false','transactional'='false');

insert into table test_init_ext (id, street) values ("3", "Hollywood");

select * from test_init_ext;

alter table test_init_ext compact 'major';

show compactions;

-- This will ONLY change the known location to a new reference location.
-- IT WILL NOT MOVE EXISTING DATA.
alter table test_init_ext set location '/warehouse/tablespace/managed/hive/reliance.db/test_init_ext';

insert into table test_init_ext (id, street) values ("4", "Hollywood");

-- Notice that only new data since location change is available.
select * from test_init_ext;

-- Change back to see old data
alter table test_init_ext set location '/warehouse/tablespace/external/hive/reliance.db/test_init_ext';

show create table test_init_ext;