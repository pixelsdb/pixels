# Presto Configuration

## Note
All the following commands are done in the master node `presto00`

## Upload
Upload the following files to the catalog of `presto-server-0.192`
- pixels.properties
- pixels-daemon-0.1.0-SNAPSHOT-full.jar

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
./bin/presto --server localhost:8080 --catalog pixels --schema pixels 

select * from test;
select count(*) from test;
select * from test where id > 2;
```