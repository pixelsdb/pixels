CFLAGS=-W

all: gen_h build

clean:
	rm *.so; rm ./lib/*.o; rm ./include/io_pixelsdb_pixels_cache_*.h

# run mvn package before to generate target jar
gen_h:
	javah -d ./include -cp ../../pixels-cache/target/pixels-cache-0.2.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.utils.RadixTreeDumper
	javah -d ./include -cp ../../pixels-cache/target/pixels-cache-0.2.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.NativeRadixIndexReader
	javah -d ./include -cp ../../pixels-cache/target/pixels-cache-0.2.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.NativeHashIndexReader
	javah -d ./include -cp ../../pixels-cache/target/pixels-cache-0.2.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.utils.RadixIndexEndianRewriter

build_radix_dumper:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux ./lib/io_pixelsdb_pixels_cache_utils_RadixTreeDumper.c -o ./lib/io_pixelsdb_pixels_cache_utils_RadixTreeDumper.o
	gcc  ${CFLAGS} -shared -fPIC -o libRadixTreeDumper.so ./lib/io_pixelsdb_pixels_cache_utils_RadixTreeDumper.o -lc

build_radix_rewriter:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux ./lib/io_pixelsdb_pixels_cache_utils_RadixIndexEndianRewriter.c -o ./lib/io_pixelsdb_pixels_cache_utils_RadixIndexEndianRewriter.o
	gcc ${CFLAGS} -shared -fPIC -o libRadixIndexEndianRewriter.so ./lib/io_pixelsdb_pixels_cache_utils_RadixIndexEndianRewriter.o -lc

build_radix_reader:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux ./lib/io_pixelsdb_pixels_cache_NativeRadixIndexReader.c -o ./lib/io_pixelsdb_pixels_cache_NativeRadixIndexReader.o
	gcc ${CFLAGS} -shared -fPIC -o libRadixIndexReader.so ./lib/io_pixelsdb_pixels_cache_NativeRadixIndexReader.o -lc

build_hash_reader:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux ./lib/io_pixelsdb_pixels_cache_NativeHashIndexReader.c -o ./lib/io_pixelsdb_pixels_cache_NativeHashIndexReader.o
	gcc ${CFLAGS} -shared -fPIC -o libHashIndexReader.so ./lib/io_pixelsdb_pixels_cache_NativeHashIndexReader.o -lc

build: build_radix_dumper build_radix_rewriter build_radix_reader build_hash_reader
