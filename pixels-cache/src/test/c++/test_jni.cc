#include <iostream>
#include "test_jni.h"
using namespace std;

static int sum = 0;

JNIEXPORT void JNICALL Java_cn_edu_ruc_iir_pixels_cache_TestJni_sayHello(JNIEnv *env, jclass cls)
{
    cout << "Hello Native\n" << endl;
}

JNIEXPORT jint JNICALL Java_cn_edu_ruc_iir_pixels_cache_TestJni_add
  (JNIEnv *env, jclass cls, jint a)
{
    sum += a;
    return sum;
}

JNIEXPORT jbyteArray JNICALL Java_cn_edu_ruc_iir_pixels_cache_TestJni_echo
  (JNIEnv *env, jclass cls, jbyteArray bytes, jint length)
{
    jbyte* data = env->GetByteArrayElements(bytes, 0);
    jbyteArray res = env->NewByteArray(length);
    env->SetByteArrayRegion(res, 0, length, data);
    return res;
}