
all: c run

clean:
	rm *.so; rm -r target/*; rm *.o

gen:
	javac -h . ../java/io/pixelsdb/pixels/cache/PixelsNativeCacheReader.java -d ./target

disc:
	javap -s -p target/yeeef/Address.class

c:
	gcc -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux yeeef_Address.c -o yeeef_Address.o
	gcc -shared -fPIC -o libaddress.so yeeef_Address.o -lc

run:
	java -cp ./target -Djava.library.path=. yeeef.Address
