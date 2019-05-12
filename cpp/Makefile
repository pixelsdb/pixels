all: JNITest

JNITest:
	$(CXX) -shared -fPIC JNITest.cc -o JNITest.so -I/usr/lib/jvm/java-1.8.0-openjdk-amd64/include/ -I/usr/lib/jvm/java-8-openjdk-amd64/include/linux/

clean:
	rm -rf ./JNITest.so
