# hdp3_upgrade_utils

Upgrading from Hive 1/2 to Hive 3 requires several metastore AND data changes to be successful.

This process and the associated scripts are meant to be used as a 'pre-upgrade' planning toolkit that can be used to make the upgrade smoother.

These scripts don't make any direct changes to hive, rather they are intended to educate and inform you of areas that need attention.  After which, it is up to you to make the adjustments manually.

## The Process

- Run the [sqoop dump utility](./hms_sqoop_dump.sh) to extract a dataset from the Metastore Database.  Sqoop will drop the dataset on HDFS.
    `./hms_sqoop_dump.sh --target-hdfs-dir /warehouse/tablespace/external/hive/<target_db>.db/hms_dump_<env> --jdbc-db-url jdbc:mysql://<host:port>/<db_name> --jdbc-user <user> --jdbc-password <password>`

- Run the [Hive HMS Schema Creation Script](./hms_dump_ddl.sql) to create the external table onto of the location you placed the sqoop extract.
    `hive --hive-var DB=<target_db> --hivevar ENV=<env> -f hms_dump_ddl.sql`
    
- Validate the dataset is visible via 'beeline'.
    `hive --hive-var DB=<target_db> --hivevar ENV=<env>`
    ```
    use ${DB};
    
    select * from hms_dump_${ENV} limit 10;
    ```
- Review each of the following scripts. Each script contains a description of it's function.
    - [Distinct Serdes](./distinct_serdes.sql)
    - [Acid Table Details](./acid_table_details.sql)
    - [Acid Table Conversions](./acid_table_conversions.sql)
    - [Missing HDFS Directories Check](./missing_table_dirs.sql)
        - The 'hdfs_path_check' field is designed to be copied into a text file an used in [Hadoop CLI](https://github.com/dstreev/hadoop-cli).  Binary Distro of hadoop-cli below.
        
## Hadoop CLI

[Hadoop CLI Project/Sources Github](https://github.com/dstreev/hadoop-cli)

Fetch the latest Binary Distro [here](https://github.com/dstreev/hadoop-cli/releases) . Unpack the hadoop.cli-x.x.x-SNAPSHOT-x.x.tar.gz and run (as root):

`./setup.sh`

This will deploy and configure the hadoop-cli.

`hadoopcli`

With no params, it will pickup your default configs for hadoop in /etc/hadoop/conf.

An interactive 'hdfs' client that can be scripted to reduce the time it takes to cycle through 'hdfs' commands.

### Usage Scenario

Test a list of directories against 'hdfs' to see if they exist.  See above 'Missing HDFS Directories Check'.

Create a text file (test.txt) and add some commands.  The last line should 'exit'.
```
test -e /user/ted
test -e /user/chuck
test -e /apps/hive/warehouse/my_db.db/some_random_tbl
exit

```
Then run the 'hadoopcli' with the text file as an init script.

`hadoopcli -i test.txt 2> missing.out`

This will pipe all 'errors' to 'missing.out'.  The 'test' command throws an error when the directory doesn't exist.
