# Pixels Index RocksDB

### Main Features
This is a single point index implementation based on RocksDB. The single point index is used to store the mapping between data and RowId. Combined with the MainIndex, it forms a complete indexing service, which is invoked by gRPC.

### Build Instructions
1. Configure the RocksDB path in `$PIXELS_HOME/etc/pixels.properties`.

2. Build this together with the root project using `mvn clean install` under the root directory of this repository.

3. Set `index.server.enabled` to `true` and add `rocksdb` to `enabled.single.point.index.schemes` in `$PIXELS_HOME/etc/pixels.properties`
to enable the index server and the rocksdb index. Index server will be started in the Pixels daemon, and it will load the indexes with the `rocksdb` index scheme.