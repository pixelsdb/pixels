# Presto-Connector


## Installation
Copy the `pixels.properties` under `pixels-presto/src/main/resources`
to the `etc/catalog` directory of Presto's home.
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
./bin/presto --server localhost:8080 --catalog pixels --schema pixels 

select * from test;
select count(*) from test;
select * from test where id > 2;
```