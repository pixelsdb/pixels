# Presto-Load Configuration

## How to use benchmark?
- Cluster Gen 30G
```
cd /home/presto/opt/tmp_data
java -jar rainbow-benchmark-0.1.0-SNAPSHOT-full.jar --data_size=30720 --thread_num=5 --directory=./data_template/
```
- Upload `data` to HDFS cluster
```
./bin/hadoop fs -put /home/presto/opt/tmp_data/data_template/rainbow_{time}_30720MB/data/* /text
```
`rainbow_{time}_30720MB` is the directory contains the data

`/text` is the directory in HDFS

## How to use pixels-load
different `LOAD` command, the same `DDL` command
- single thread
`pom.xml` change **mainClass** with 'cn.edu.ruc.iir.pixels.load.single.Main'
- multiple thread
`pom.xml` change **mainClass** with 'cn.edu.ruc.iir.pixels.load.multi.Main'

## Pixels consumer command line tool
1> Start `pixels-metadata` thread
```
java -jar -Dio.netty.leakDetection.level=advanced -Drole=main pixels-damon-0.1.0-SNAPSHOT-full.jar metadata
```
2> Start `pixels-load` thread
```
java -jar pixels-load-0.1.0-SNAPSHOT-full.jar
```
`Note` use `DDL -h` or `LOAD -h`, you can see the usages of the command
- DDL Command
```
DDL -s {schema_file} -d {db_name}
```
- LOAD Command *single thread*
```
LOAD -f {format} -o {original_data_path} -d {db_name} -t {table_name} -n {row_num} -r {row_regex}

pixels> LOAD -f pixels -o hdfs://dbiir01:9000/pixels/pixels/test_105/source -d pixels -t test_105 -n 300000 -r \t
```
- LOAD Command *multiple thread*
```
LOAD -f {format} -o {original_data_path} -d {db_name} -t {table_name} -n {row_num} -r {row_regex} -c {consumer_thread_num} -p {producer}

pixels> LOAD -f pixels -o hdfs://dbiir01:9000/pixels/pixels/test_105/source -d pixels -t test_105 -n 300000 -r \t -c 4 -p false
pixels> LOAD -f pixels -o hdfs://dbiir01:9000/pixels/pixels/test_105/source -d pixels -t test_105 -n 300000 -r \t -c 4

{producer} is optional, default false.

```

## Presto Command
- execute query
```
cd /home/iir/opt/presto-server-0.192
./bin/presto --server localhost:8080 --catalog pixels-presto --schema pixels
show tables;
```

## Orc
- Use `hive` to create tables such as text,  and insert data from text(like the following *.sql)
    - text_ddl.sql, orc_ddl.sql
    - load_ddl.sql
```
./bin/hive
```

- presto
```
./bin/presto --server localhost:8080 --catalog hive --schema default
```

## 查看Logs
Go to path `/home/iir/opt/presto-server-0.192/data/var/log/server.log` 