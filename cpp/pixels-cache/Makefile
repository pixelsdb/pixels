CFLAGS=-W

all: gen_h build

clean:
	rm *.so; rm ./lib/*.o; rm ./include/io_pixelsdb_pixels_cache_legacy_*.h

# run mvn package before to generate target jar
gen_h:
	javah -d ./include -cp ../../pixels-cache/target/pixels-cache-0.2.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.legacy.utils.RadixTreeDumper
	javah -d ./include -cp ../../pixels-cache/target/pixels-cache-0.2.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.legacy.NativeRadixIndexReader
	javah -d ./include -cp ../../pixels-cache/target/pixels-cache-0.2.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.legacy.NativeHashIndexReader
	javah -d ./include -cp ../../pixels-cache/target/pixels-cache-0.2.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.legacy.utils.RadixIndexEndianRewriter

build_radix_dumper:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux ./lib/UtilsRadixTreeDumper.c -o ./lib/UtilsRadixTreeDumper.o
	gcc  ${CFLAGS} -shared -fPIC -o libRadixTreeDumper.so ./lib/UtilsRadixTreeDumper.o -lc

build_radix_rewriter:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux ./lib/UtilsRadixIndexEndianRewriter.c -o ./lib/UtilsRadixIndexEndianRewriter.o
	gcc ${CFLAGS} -shared -fPIC -o libRadixIndexEndianRewriter.so ./lib/UtilsRadixIndexEndianRewriter.o -lc

build_radix_reader:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux ./lib/NativeRadixIndexReader.c -o ./lib/NativeRadixIndexReader.o
	gcc ${CFLAGS} -shared -fPIC -o libRadixIndexReader.so ./lib/NativeRadixIndexReader.o -lc

build_hash_reader:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux ./lib/NativeHashIndexReader.c -o ./lib/NativeHashIndexReader.o
	gcc ${CFLAGS} -shared -fPIC -o libHashIndexReader.so ./lib/NativeHashIndexReader.o -lc

build: build_radix_dumper build_radix_rewriter build_radix_reader build_hash_reader
