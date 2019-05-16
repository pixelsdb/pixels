all: PixelsCacheReader

PixelsCacheReader: PixelsCacheReader.cc MemoryMappedFile
	$(CXX) -shared -fPIC $@.cc -o lib_pixels.so -I/usr/lib/jvm/java-1.8.0-openjdk-amd64/include/ -I/usr/lib/jvm/java-8-openjdk-amd64/include/linux/

MemoryMappedFile:
	$(CXX) -c $@.cc

clean:
	rm -rf ./lib_pixels.so ./*.o
