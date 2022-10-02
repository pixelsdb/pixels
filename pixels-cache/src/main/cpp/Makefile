CFLAGS=-W

all: c run

clean:
	rm *.so; rm -r target/*; rm *.o

gen:
	javah -d . -cp ../../../target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.CacheIndexSerializer
	javah -d . -cp ../../../target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.PixelsNativeCacheReader
	javah -d . -cp ../../../target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.HashIndexReader
	javah -d . -cp ../../../target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.CacheIndexRewriter

disc:
	javap -s -p target/yeeef/Address.class

serialize:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux io_pixelsdb_pixels_cache_CacheIndexSerializer.c -o io_pixelsdb_pixels_cache_CacheIndexSerializer.o
	gcc  ${CFLAGS} -shared -fPIC -o ../../../libserializer.so io_pixelsdb_pixels_cache_CacheIndexSerializer.o -lc

reader:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux io_pixelsdb_pixels_cache_PixelsNativeCacheReader.c -o io_pixelsdb_pixels_cache_PixelsNativeCacheReader.o
	gcc ${CFLAGS} -shared -fPIC -o ../../../libreader.so io_pixelsdb_pixels_cache_PixelsNativeCacheReader.o -lc

hash_index:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux io_pixelsdb_pixels_cache_HashIndexReader.c -o io_pixelsdb_pixels_cache_HashIndexReader.o
	gcc ${CFLAGS} -shared -fPIC -o ../../../libHashIndexReader.so io_pixelsdb_pixels_cache_HashIndexReader.o -lc

rewriter:
	gcc ${CFLAGS} -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux io_pixelsdb_pixels_cache_CacheIndexRewriter.c -o io_pixelsdb_pixels_cache_CacheIndexRewriter.o
	gcc ${CFLAGS} -shared -fPIC -o ../../../librewriter.so io_pixelsdb_pixels_cache_CacheIndexRewriter.o -lc

run:
	java -cp ./target -Djava.library.path=. yeeef.Address
