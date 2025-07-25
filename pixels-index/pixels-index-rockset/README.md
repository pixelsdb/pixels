# Pixels Index Rockset

### Main Features
This is a single point index implementation based on Rockset. It is the cloud version of RocksDBIndex and is used to add indexes for data stored in S3 buckets on AWS EC2 instances.

### Build Instructions
1. Configure the bucketName, s3Prefix, localDbPath, persistentCachePath, persistentCacheSizeGB, readOnly in `$PIXELS_HOME/etc/pixels.properties`.

2. RocksetIndex depends on the corresponding C++ module for compilation. 
Before compiling the Java project, you need to first compile the C++ module to generate `libpixels-index-rockset.so` following the instructions [HERE](../../cpp/pixels-index/pixels-index-rockset/README.md).

3. Build this together with the root project using `mvn clean install` under the root directory of this repository.

4. Set `index.server.enabled` to `true` and add `rockset` to `enabled.single.point.index.schemes` in `$PIXELS_HOME/etc/pixels.properties`
   to enable the index server and the rockset index. Index server will be started in the Pixels daemon, and it will load the indexes with the `rockset` index scheme.