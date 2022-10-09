CFLAGS=-W

all: c run

clean:
	rm *.so; rm -r target/*; rm *.o

# run mvn package before to generate target jar
gen:
	javah -d ./src/main/cpp -cp ./target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.utils.RadixTreeDumper
	javah -d ./src/main/cpp -cp ./target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.NativeRadixIndexReader
	javah -d ./src/main/cpp -cp ./target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.NativeHashIndexReader
	javah -d ./src/main/cpp -cp ./target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.utils.RadixIndexEndianRewriter

disc:
	javap -s -p target/yeeef/Address.class

radix_dumper:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux src/main/cpp/io_pixelsdb_pixels_cache_utils_RadixTreeDumper.c -o src/main/cpp/io_pixelsdb_pixels_cache_utils_RadixTreeDumper.o
	gcc  ${CFLAGS} -shared -fPIC -o libRadixTreeDumper.so src/main/cpp/io_pixelsdb_pixels_cache_utils_RadixTreeDumper.o -lc

radix_rewriter:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux src/main/cpp/io_pixelsdb_pixels_cache_utils_RadixIndexEndianRewriter.c -o src/main/cpp/io_pixelsdb_pixels_cache_utils_RadixIndexEndianRewriter.o
	gcc ${CFLAGS} -shared -fPIC -o libRadixIndexEndianRewriter.so src/main/cpp/io_pixelsdb_pixels_cache_utils_RadixIndexEndianRewriter.o -lc

radix_reader:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux src/main/cpp/io_pixelsdb_pixels_cache_NativeRadixIndexReader.c -o src/main/cpp/io_pixelsdb_pixels_cache_NativeRadixIndexReader.o
	gcc ${CFLAGS} -shared -fPIC -o libRadixIndexReader.so src/main/cpp/io_pixelsdb_pixels_cache_NativeRadixIndexReader.o -lc

hash_reader:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux src/main/cpp/io_pixelsdb_pixels_cache_NativeHashIndexReader.c -o src/main/cpp/io_pixelsdb_pixels_cache_NativeHashIndexReader.o
	gcc ${CFLAGS} -shared -fPIC -o libHashIndexReader.so src/main/cpp/io_pixelsdb_pixels_cache_NativeHashIndexReader.o -lc


run:
	java -cp ./target -Djava.library.path=. yeeef.Address
