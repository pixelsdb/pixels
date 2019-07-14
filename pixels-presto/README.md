# Presto-Connector


## Installation
Copy `pixels-presto.properties` to the catalog directory of Presto
Build Pixels by `mvn package`, copy and unzip `pixels-presto-0.1.0-SNAPSHOT.zip`
to the plugin directory of Presto

## Run Metadata Server
Run the server, we need to use the `Root` command
```sh
java -Dio.netty.leakDetection.level=advanced -Drole=main -jar pixels-daemon-0.1.0-SNAPSHOT-full.jar metadata
```
```sh
VM option: -Dio.netty.leakDetection.level=advanced -Drole=main 
Program arguements: metadata
```
## Run Presto
Run the presto client, we should do the following things:
- stop the clusters
```sh
./sbin/stop-all.sh
```

- move the `plugins` to each node
```sh
./sbin/mv-plugins.sh
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