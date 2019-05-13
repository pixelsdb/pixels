all: NativePixelsCacheReader

NativePixelsCacheReader:
	$(CXX) -shared -fPIC $@.cc -o lib_pixels.so -I/usr/lib/jvm/java-1.8.0-openjdk-amd64/include/ -I/usr/lib/jvm/java-8-openjdk-amd64/include/linux/

clean:
	rm -rf ./lib_pixels.so
