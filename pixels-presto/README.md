# Presto Configuration

## Note
All the following commands are done in the master node `presto00`

## Upload
Upload the following files to the catalog of `presto-server-0.192`
- pixels.properties
- pixels-daemon-0.1.0-SNAPSHOT-full.jar
- pixels-load-0.1.0-SNAPSHOT.jar

## Run ETL Tool
- DDL

`DDL -s schema_file -d db_name`
```
DDL -s /home/tao/software/station/bitbucket/pixels/pixels-load/src/main/resources/DDL.txt -d pixels
```
- LOAD

`LOAD -p data_path -s schema_file -f hdfs_file`
```
LOAD -p /home/tao/software/station/bitbucket/pixels/pixels-load/src/main/resources/data/ -s /home/tao/software/station/bitbucket/pixels/pixels-load/src/main/resources/Test.sql -f hdfs://presto00:9000/po_compare/test.pxl
```

## Run Metadata Server
Run the server, we need to use the `Root` command
```sh
java -Drole=main -jar pixels-daemon-0.1.0-SNAPSHOT-full.jar metadata
```

## Run Presto
Run the presto client, we should do the following things:
- stop the clusters
```sh
./sbin/stop-all.sh
```

- move the `plugins` to each node
```sh
./bin/mv-plugins.sh
```
- start the clusters
```sh
./sbin/start-all.sh
```
- execute
```sh
./bin/presto --server localhost:8080 --catalog pixels-presto --schema pixels 

select * from test;
select count(*) from test;
select * from test where id > 2;
```