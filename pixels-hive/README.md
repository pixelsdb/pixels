# Pixels-Hive
SerDe is short for “Serializer and Deserializer.”

Hive uses SerDe (and !FileFormat) to read and write table rows.

HDFS files –> InputFileFormat –> <key, value> –> Deserializer –> Row object

Row object –> Serializer –> <key, value> –> OutputFileFormat –> HDFS files

## Note
Source code in this module partially refers to [ORC SerDe](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/orc/OrcSerde.java).

## Usage
./bin/hive

1. In hive cli:
```sql
hive> add jar {PATH}/pixels-hive-0.1.0-SNAPSHOT-full.jar
```
You can also put `pixels-hive-0.1.0-SNAPSHOT-full.jar` in the path `apache-hive-2.1.1-bin/auxlib/`, and hive will load the jar when launching.

2. Create tables in hive with the syntax in 
[Hive Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Create/Drop/Alter/UseDatabase).
But use the following `ROW FORMAT` and `TABLEPROPERTY`:
```SQL
ROW FORMAT SERDE 'cn.edu.ruc.iir.pixels.hive.PixelsSerDe'
STORED AS INPUTFORMAT
  'cn.edu.ruc.iir.pixels.hive.mapred.PixelsInputFormat'
OUTPUTFORMAT
  'cn.edu.ruc.iir.pixels.hive.mapred.PixelsOutputFormat'
LOCATION '/any_empty_dir/'
TBLPROPERTIES ("bind.pixels.table"="schema_name.table_name");
```
`LOCATION` is current necessary, but it does not have a meaning
and can to set to any empty dir you like. `bind.pixels.table` specifies
which table in pixels to bind this hive table. Replace `schema_name`
and `table_name` with the right schema and table name in pixels.

**BUT** be careful: It is a good idea to create an EXTERNAL table, internal table will delete data
when the table is dropped. If there is data in the location, you will lost it.

3. Load data by pixels-load and then execute queries in hive. 
Currently, we have only implemented `PixelsInputFormat` for hive,
so that data can not be loaded through Hive.
Before executing a query, set max reducer number and `hive.input.format` in the session:
```sh
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=16;
```
