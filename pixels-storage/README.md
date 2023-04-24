# Pixels Storage

This folder contains the providers of the file/object storage systems.
Each storage provider provides the adapters and I/O methods for a type of underlying
file/object storage system that is used to store data in Pixels.
Queries will load and call the providers to get access to the underlying storage.
- `pixels-storage-s3` provides s3-compatible object storage systems such as AWS S3 and MinIO.
- `pixels-storage-gcs` provides google cloud object storage.
- `pixels-storage-hdfs` provides hadoop-compatible file systems such as HDFS.
- `pixels-storage-localfs` provides local file systems.
- `pixels-storage-mock` provides a mock storage system for debugging.
- `pixels-storage-redis` provides redis key-value storage.

## Usage
Storage provider can be used in either of the following ways:
1. Put the compiled jar and its dependencies in the CLASSPATH of you program.
2. If your program is build by maven, you can also add the storage provider as dependency.
Note that if multiple storage providers are used in the same project/module, make sure that your maven plugins
merge the resources in `META-INF/services`. For example, in maven-shade-plugin, it can be done like this:
```xml
<project>
    ...
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.2.2</version>
                <executions>
                    <execution>
                        <id>your-id</id>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            ...
                            <transformers>
                                <!-- ServicesResourceTransformer merges the resources in META-INF/services -->
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```
