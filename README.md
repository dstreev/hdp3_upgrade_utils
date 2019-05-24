# hdp3_upgrade_utils

Upgrading from Hive 1/2 to Hive 3 requires several metastore AND data changes to be successful.

This process and the associated scripts are meant to be used as a 'pre-upgrade' planning toolkit to make the upgrade smoother.

These scripts don't make any direct changes to hive, rather they are intended to educate and inform you of areas that need attention.  After which, it is up to you to make the adjustments manually.

We'll use a combination of Hive SQL and an interactive HDFS client [Hadoop-Cli](https://github.com/dstreev/hadoop-cli) to combine information from an extract of the Metastore DB and the contents of HDFS.

## Calling Hive

We use Hive throughout this process.  The process has been validated against Hive3, using Beeline against LLAP.  To use against LLAP in HDP 2.6, you'll need to build a 'beeline' wrapper to connect automatically.  The output of 'beeline' will be a little different then the output of 'hive cli'.  So I recommend using 'beeline' in HDP 2.6 for this process since the pipeline has particular dependencies.

### Environment variables for this process

#### Hive
- HDP 3 - `export HIVE_ALIAS="hive -c llap"`
- HDP 2.6 - `export HIVE_ALIAS="beeline -u ${jdbc_url_to_hive}"`

#### Script vars

> Set the following environment variable to assist with these scripts
```
export TARGET_DB=<target_db>
export DUMP_ENV=<dump_env>
export OUTPUT_DIR=<base_output_dir>
export EXTERNAL_WAREHOUSE_DIR=<ext_wh_dir>

# For Example:
export TARGET_DB=mycompany
export DUMP_ENV=dev
export EXTERNAL_WAREHOUSE_DIR=/apps/hive/warehouse
export OUTPUT_DIR=/tmp

```


## The Process

- Run the [sqoop dump utility](./hms_sqoop_dump.sh) to extract a dataset from the Metastore Database.  Sqoop will drop the dataset on HDFS.
    > NOTE: If you have defined 'lower_case_table_names' in my.cnf for MySql/MariaDB, add the --lower option to the call to select the correct extract sql for 'Sqoop'.

    ```
    ./hms_sqoop_dump.sh --target-hdfs-dir \
    /warehouse/tablespace/external/hive/<target_db>.db/hms_dump_<env> \
    --jdbc-db-url jdbc:mysql://<host:port>/<db_name> \
    --jdbc-user <user> --jdbc-password <password>
    ```
    > The 'target-hdfs-dir' is where you'll define the 'external' table for this dataset.  The location should coincide with the standard external dataset location.
- Run the [Hive HMS Schema Creation Script](./hms_dump_ddl.sql) to create the external table onto of the location you placed the sqoop extract.
    ```
    ${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} -f hms_dump_ddl.sql
    ```
- Validate the dataset is visible via 'beeline'.
    ```
    ${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV}
    ```
    In Beeline:
    ```
    use ${DB};
    
    select * from hms_dump_${ENV} limit 10;
    ```
- Review each of the following scripts. Each script contains a description of it's function.
    
    - [Distinct Serdes](./distinct_serdes.sql)
       
        `${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} -f distinct_serdes.sql`
       
    - [Find table with Serde x](./serde_tables.sql)
       
        `${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} --hivevar SERDE=<serde> -f serde_tables.sql`
       
    - [Check Partition Location](./check_partition_location.sql)
        > Many assumptions are made about partition locations.  When these location aren't standard, it may have an effect on other migration processes and calculations.  This script will help identify that impact.
        
        `${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} -f check_partition_location.sql`
                
    - [Non-Managed Table Locations](./external_table_location.sql)
        > Determine the overall size/count of the tables locations
        
        `${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} \
                 --showHeader=false --outputformat=tsv2 -f external_table_location.sql`
        
        ```
        ${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} \
        --showHeader=false --outputformat=tsv2 -f external_table_location.sql | \
        cut -f 3 | sed -r "s/(^.*)/count \1/" | \
        hadoopcli -stdin -s | sed -r "s/[ ]{2,}/\t/g" | sed -r "s/\s\//\t\//g" | sed -r "s/^\t//g"> ${OUTPUT_DIR}/external_table_stats.txt  
        ```
        > Copy the above file to HDFS
        `hdfs dfs -copyFromLocal ${OUTPUT_DIR}/external_table_stats.txt ${EXTERNAL_WAREHOUSE_DIR}/${DB}.db/dir_size_${ENV}`
    - [Managed Table Locations](./managed_table_location.sql)
        > Determine the overall size/count of the tables locations
        
        `${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} \
                 --showHeader=false --outputformat=tsv2 -f managed_table_location.sql`
        
        ```
        ${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} \
        --showHeader=false --outputformat=tsv2 -f managed_table_location.sql | \
        cut -f 3 | sed -r "s/(^.*)/count \1/" | \
        hadoopcli -stdin -s | sed -r "s/[ ]{2,}/\t/g" | sed -r "s/\s\//\t\//g" | sed -r "s/^\t//g" > ${OUTPUT_DIR}/managed_table_stats.txt  
        ```
        > Copy the above file to HDFS
        `hdfs dfs -copyFromLocal ${OUTPUT_DIR}/managed_table_stats.txt ${EXTERNAL_WAREHOUSE_DIR}/${DB}.db/dir_size_${ENV}`
    - For LARGE Hive Installations, build an alter Migration Script
        > With the data collected from 'External/Managed Table Locations', we can run the following and get table and db sizes.
        ```
        ${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} \
                --showHeader=false --outputformat=tsv2 -f size_of_dbs.sql
        ```
        
        
        
    - [Acid Table Details](./acid_table_details.sql)
        > These details will provide an indication of how many tables are eligible for compaction before the upgrade.  As required before the upgrade, ALL ACIDv1 tables need to be compacted (MAJOR).  ACIDv1 delta files are NOT forward compatible.
        > This list can provide a clue to the amount of processing that will be required by the compactor before the upgrade.  If this list is large, the pre-upgrade script should be run several days in advance of the upgrade to process any outstand 'major' compactions.  And then run at intervals leading up to the upgrade, to reduce the time it takes for the pre-upgrade processing time when the upgrade is started.
        
        `${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} -f acid_table_details.sql`
        
    - [Acid Tables Location](./acid_table_location_status.sql)
        
        `${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} -f acid_table_location_status.sql`
            
    - [Table Migration Check](./table_migration_check.sql)
        > This will produce a list of tables and directories that need their ownership checked.  If they are owned by 'hive', these 'managed' tables will be migrated to the new warehouse directory for Hive3.
        
        
        `${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} -f table_migration_check.sql`
        

        ```        
        ${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} \
        --showHeader=false --outputformat=tsv2 -f table_migration_check.sql | \
        cut -f 1,2,5,6 | sed -r "s/(^.*)(\/apps.*)/lsp -c \"\1\" -f user,group,permissions_long,path \2/" | \
        hadoopcli -stdin -s > ${OUTPUT_DIR}/migration_check.txt
        ```
        
    - [Acid Table Conversions](./acid_table_conversions.sql)
        > This script provides a bit more detail then [Table Migration Check](./table_migration_check.sql), which only looks for tables in the standard location.
        
        
        `${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} -f acid_table_conversions.sql`
        
    - [Missing HDFS Directories Check](./missing_table_dirs.sql)
        > The beeline output can be captured and pushed into the 'HadoopCli' for processing.  The following command will generate a script that can also be run with '-f' option in 'HadoopCli' to create the missing directories.
        > Even though we push this through hadoopcli for the hdfs test function, this will take some time to run.  If you want to see the progress, open another window session and tail the 'hcli_mkdir.txt' file.
        
        `${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} -f missing_table_dirs.sql`
        
        ```
        ${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} \
        --showHeader=false --outputformat=tsv2  -f missing_table_dirs.sql | \
        hadoopcli -stdin -s 2>&1 >/dev/null | cut -f 4 | \
        sed 's/^/mkdir -p /g' > ${OUTPUT_DIR}/hcli_mkdir.txt
        ```
        > Review the output file 'hcli_mkdir.txt', edit if necessary and process through 'hadoopcli'.
        ```
        hadoopcli -r hcli_mkdir.txt
        ```
    - [Conversion Table Directories](./table_dirs_for_conversion.sql) Locate Files that will prevent tables from Converting to ACID.
        > The 'alter' statements used to create a transactional table require a specific file pattern for existing files.  Files that don't match this, will cause issues with the upgrade.
        >> NOTE: The current test is for *.c000 ONLY.  The sql needs to be adjusted to match a different regex.
        > Get a list of table directories to check and run that through the 'Hadoop Cli' below to locate the odd files.
        
        `${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} -f table_dirs_for_conversion.sql`
        
        ```
        ${HIVE_ALIAS} --hivevar DB=${TARGET_DB} --hivevar ENV=${DUMP_ENV} \
        --showHeader=false --outputformat=tsv2  -f table_dirs_for_conversion.sql | \
        sed -r "s/(^.*)/lsp -R -F <pattern> \1/" | hadoopcli -stdin -s >> ${OUTPUT_DIR}/bad_file_patterns.txt         
        ```
        >> Figure out which pattern to use through testing with 'lsp' in [Hadoop Cli](https://github.com/dstreev/hadoop-cli)
        >>> `lsp -R -F .*.c000 <path>` will recurse the path looking for files with a 'c000' extension.
        
## Hadoop CLI

An interactive/scripted 'hdfs' client that can be scripted to reduce the time it takes to cycle through 'hdfs' commands.  

[Hadoop CLI Project/Sources Github](https://github.com/dstreev/hadoop-cli)

Note: As of this writing, version 2.0.12-SNAPSHOT(or later) and above is required for this effort.

Fetch the latest Binary Distro [here](https://github.com/dstreev/hadoop-cli/releases) . Unpack the hadoop.cli-x.x.x-SNAPSHOT-x.x.tar.gz and run (as root) the setup from the extracted folder.

`./setup.sh`

Launch the application without parameters will pickup your default configs, just like `hdfs` or `hadoop` command line applications.

`hadoopcli`

### Usage Scenario

#### STDIN Processing

The Hadoop Cli can process `stdin`.  So it can be part of a bash pipeline.  In this case, we run a query in beeline, output the results and create another file with our target commands.

```
hive -c llap --hivevar DB=citizens --hivevar ENV=qa \
--showHeader=false --outputformat=tsv2  -f test.sql | \
hadoopcli -stdin 2>&1 >/dev/null | cut -f 4 | \
sed 's/^/mkdir -p /g' > hcli_mkdir.txt
```

#### File Based Script

Test a list of directories against 'hdfs' to see if they exist.  See above 'Missing HDFS Directories Check'.

Create a text file (test.txt) and add some commands.  The last line should 'exit' followed by an empty line.
```
test -e /user/ted
test -e /user/chuck
test -e /apps/hive/warehouse/my_db.db/some_random_tbl
exit

```
Then run the 'hadoopcli' with the text file as an init script.

`hadoopcli -f test.txt 2> missing.out`

This will pipe all 'errors' to 'missing.out'.  The 'test' command throws an error when the directory doesn't exist.
