CFLAGS=-W

all: c run

clean:
	rm *.so; rm -r target/*; rm *.o

# run mvn package before to generate target jar
gen:
	javah -d ./src/main/cpp -cp ./target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.utils.RadixTreeDumper
# 	javah -d ./src/main/cpp -cp ./target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.PixelsNativeCacheReader
	javah -d ./src/main/cpp -cp ./target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.NativeHashIndexReader
# 	javah -d ./src/main/cpp -cp ./target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.CacheIndexRewriter

disc:
	javap -s -p target/yeeef/Address.class

radix_dumper:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux src/main/cpp/io_pixelsdb_pixels_cache_utils_RadixTreeDumper.c -o src/main/cpp/io_pixelsdb_pixels_cache_utils_RadixTreeDumper.o
	gcc  ${CFLAGS} -shared -fPIC -o libRadixTreeDumper.so src/main/cpp/io_pixelsdb_pixels_cache_utils_RadixTreeDumper.o -lc

# reader:
# 	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux io_pixelsdb_pixels_cache_PixelsNativeCacheReader.c -o io_pixelsdb_pixels_cache_PixelsNativeCacheReader.o
# 	gcc ${CFLAGS} -shared -fPIC -o ../../../libreader.so io_pixelsdb_pixels_cache_PixelsNativeCacheReader.o -lc

hash_reader:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux src/main/cpp/io_pixelsdb_pixels_cache_NativeHashIndexReader.c -o src/main/cpp/io_pixelsdb_pixels_cache_NativeHashIndexReader.o
	gcc ${CFLAGS} -shared -fPIC -o libHashIndexReader.so src/main/cpp/io_pixelsdb_pixels_cache_NativeHashIndexReader.o -lc

# rewriter:
# 	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux io_pixelsdb_pixels_cache_CacheIndexRewriter.c -o io_pixelsdb_pixels_cache_CacheIndexRewriter.o
# 	gcc ${CFLAGS} -shared -fPIC -o ../../../librewriter.so io_pixelsdb_pixels_cache_CacheIndexRewriter.o -lc

run:
	java -cp ./target -Djava.library.path=. yeeef.Address
