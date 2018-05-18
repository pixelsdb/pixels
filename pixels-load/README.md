# Presto-Load Configuration

## Cluster Gen 30G
cd /home/presto/opt/tmp_data
java -jar rainbow-benchmark-0.1.0-SNAPSHOT-full.jar --data_size=30720 --thread_num=5 --directory=./data_template/

## IDEA Gen 4M
java -jar rainbow-benchmark-0.1.0-SNAPSHOT-full.jar --data_size=4 --thread_num=4 --directory=./data_template/

## HDFS
./bin/hadoop fs -put /home/presto/opt/tmp_data/data_template/rainbow_201805161438200_30720MB/data/* /msra/text

## Pixels Load
java -jar pixels-load-0.1.0-SNAPSHOT-full.jar
1> presto-load DDL
DDL -s ./test30G_pixels/presto_ddl.sql -d pixels
2> presto-load LOAD
LOAD -p ./test30G_pixels/data/ -s ./test30G_pixels/presto_ddl.sql -f hdfs://presto00:9000/pixels/test30G_pixels/
3> presto
cd /home/presto/opt/presto-server-0.192
./bin/presto --server localhost:8080 --catalog pixels-presto --schema pixels

## Orc
./bin/hive
1> text_ddl.sql, orc_ddl.sql

2> load_ddl.sql

3> presto
./bin/presto --server localhost:8080 --catalog hive --schema default


## 查看Logs
/home/presto/opt/presto-server-0.192/data/var/log/