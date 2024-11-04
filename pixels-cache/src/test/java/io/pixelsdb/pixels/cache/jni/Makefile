all: test_jni

test_jni:
	$(CXX) -shared -fPIC $@.cpp -o $@.so -I${JAVA_HOME}/include/ -I${JAVA_HOME}/include/linux/

clean:
	rm -r ./test_jni.so
