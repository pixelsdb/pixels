# Pixels-Hive
SerDe is a short name for “Serializer and Deserializer.”

Hive uses SerDe (and !FileFormat) to read and write table rows.

HDFS files –> InputFileFormat –> <key, value> –> Deserializer –> Row object

Row object –> Serializer –> <key, value> –> OutputFileFormat –> HDFS files

## Note
This module is referred to [ORC](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/orc/OrcSerde.java).

## Run Hive Configuration
./bin/hive

hive>add jar {PATH}/pixels-hive-0.1.0-SNAPSHOT-full.jar

hive>`create sql` with the text in `resources/pixels-ddl_single.sql`

hive>`query`

`Hive SerDe`: You can also mv `pixels-hive-0.1.0-SNAPSHOT-full.jar` to the path`apache-hive-2.1.1-bin/auxlib/`, and hive will load the jar when beginning.

## QA
1. Q: "FAILED: SemanticException [Error 10055]: Output Format must implement HiveOutputFormat, otherwise it should be either IgnoreKeyTextOutputFormat or SequenceFileOutputFormat"

   A: Add `cn.edu.ruc.iir.pixels.hive.PixelsNewOutputFormat`.
   
2. Q: Wrong FS in hdfs
   A: Add `core-site.xml` and `hdfs-site.xml` to `resources`.