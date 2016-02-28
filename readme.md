#Hbase TIdx
The solution of Hbase Update-Time's Secondary Index based on [Apache Phoenix](https://phoenix.apache.org/).

#Why To Use
If you use some id to the hbase's rowkey, but you also need to scan table by the record's update-time, You can use Hbase Tidx.
 
#Feature
- No Concurrent-Write Problem With The Pheonix Local Secondary Index
- Automaticly Update Index Table With HBase Coprocessor
- Native Scan Support Without The Phoenix SQL
- Hive Integration

#Build
```bash
git clone ...
cd ...
mvn clean package
```

If you use hdp, you can use the hdp profile:

```bash
mvn clean package -Phdpxxx
```

If you use other phoenix/hbase/hive, you can edit the pom.xml.

#How To Use
##Prepare
### Install
- install apache phoenix, see [details](https://phoenix.apache.org/installation.html)
- install phoenix secondary index, see [details](https://phoenix.apache.org/secondary_indexing.html)
- install hbase tidx: build firstly, then put the hbase-tidx-core-xxx.jar to the hbase lib directory

### Create Table And Local Index In Phoenix
```bash
create table t1 (key varchar primary key, t unsigned_long, a varchar) VERSIONS=1;
create table t1_local_index_0 on t1(t);
```

### Add RegionObserver To The DataTable And IndexTable
```bash

hbase shell
# --------add data update region observer--------
disable 'T1'
alter 'T1', 'coprocessor'=>'|com.github.dryangkun.hbase.tidx.TxDataRegionObserver|1001|tx.time.col=0:T,tx.phoenix.index.id=0'
enable 'T1'
# --------add index scan region observer --------
disable '_LOCAL_IDX_T1'
alter '_LOCAL_IDX_T1', 'coprocessor'=>'|com.github.dryangkun.hbase.tidx.TxRegionObserver|1001|tx.time.col=0:T,tx.phoenix.index.id=0'
enable '_LOCAL_IDX_T1'
```
observer arguments:

- tx.time.col: the update-time's family:qualifier, eg 0:T
- tx.pidx.id: the phoenix local index id

##Update Data Table
see [TxConcurrencyTest](./tidx-core/src/test/java/com/github/dryangkun/hbase/tidx/TxConcurrencyTest.java),
 no difference with directly put data table.

##Scan Index Table
see [TxScanExample](./tidx-core/src/test/java/com/github/dryangkun/hbase/tidx/TxScanExample.java).

scan index table, but return the data table result.

time-check: if there is different between index update-time and data update-time, then don't return the record.

when time-check is set to false, then the returned-data-table-result contains special virtual faimly:qualifier(0:^T).

##Mapreduce
see [TxJobExample](./tidx-core/src/main/java/com/github/dryangkun/hbase/tidx/mapreduce/TxJobExample.java).

you can run the example:

```bash
java -cp`hadoop classpath`:`hbase mapredcp`:hbase-tidx-core-xxx.jar com.github.dryangkun.hbase.tidx.mapreduce.TxJobExample
```

##Hive Integration
### Install
configure 'hive.aux.jars.path' in hive-site.xml:

```xml
<property>
    <name>hive.aux.jars.path</name>
    <value>
        file:///.../hbase-tidx-hive-xxx.jar,
        file:///.../hbase-tidx-core-xxx.jar,
        file:///phoenix-path/phoenix-core-xxx.jar,
        hbase-mapred-jars # get from shell `hbase mapredcp`
    </value>
</property>
```

### Create Hive Table
```sql
create external table hbase_t1(
key string, 
t bigint, 
a string) 
stored by 'com.github.dryangkun.hbase.tidx.hive.HBaseStorageHandler'
with serdeproperties ("hbase.columns.mapping"=":key,0:T#b,0:A")
tblproperties("hbase.table.name"="T1","tx.hive.time.col"="0:T","tx.hive.pidx.id"="0");
```
other properties is the same with [Hive HBaseIntegration](https://cwiki.apache.org/confluence/display/Hive/HBaseIntegration)

usage:

```sql
select * from hbase_t1 where t >= ... and t < ...
```
not support "between ... and ..." now.

#Limitation
##Don't guarantee the consistency between data-table and index-table
because update index-table after data-table put success, so if update index-table fail, there is not consistent.

you can retry put operation.

only if the with-max-timestamp put success when there are same rowkey's puts, then update index-table.

there is success deleting rowkey in data-table, then delete index-table.

##Don't contains the same rowkey between put and delete in one batch operations
that too complex.

##setBatch not support when scan idex-table
