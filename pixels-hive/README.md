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

2. Create tables in hive with the text in `resources/pixels-ddl_single.sql`

3. Load data by pixels-load and then execute queries. Currently, we only implemented PixelsInputFormat for hive. 

## FAQ
1. Q: "FAILED: SemanticException [Error 10055]: Output Format must implement HiveOutputFormat, otherwise it should be either IgnoreKeyTextOutputFormat or SequenceFileOutputFormat"
   
   A: `PixelsOutputFormat` should implements `HiveOutputFormat<NullWritable, PixelsSerDe.PixelsSerdeRow>`.
   
2. Q: Wrong FS in hdfs
   
   A: Add `core-site.xml` and `hdfs-site.xml` to `resources`.