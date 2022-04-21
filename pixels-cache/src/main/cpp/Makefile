
all: c run

clean:
	rm *.so; rm -r target/*; rm *.o

gen:
	javah -d . -cp ../../../target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.CacheIndexSerializer
	javah -d . -cp ../../../target/pixels-cache-0.1.0-SNAPSHOT.jar io.pixelsdb.pixels.cache.PixelsNativeCacheReader


disc:
	javap -s -p target/yeeef/Address.class

serialize:
	gcc -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux io_pixelsdb_pixels_cache_CacheIndexSerializer.c -o io_pixelsdb_pixels_cache_CacheIndexSerializer.o
	gcc -shared -fPIC -o ../../../libcache.so io_pixelsdb_pixels_cache_CacheIndexSerializer.o -lc

reader:
	gcc -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux io_pixelsdb_pixels_cache_PixelsNativeCacheReader.c -o io_pixelsdb_pixels_cache_PixelsNativeCacheReader.o
	gcc -shared -fPIC -o ../../../libreader.so io_pixelsdb_pixels_cache_PixelsNativeCacheReader.o -lc

run:
	java -cp ./target -Djava.library.path=. yeeef.Address
