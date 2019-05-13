#include <iostream>
#include "NativePixelsCacheReader.h"
using namespace std;

JNIEXPORT jbyteArray JNICALL Java_cn_edu_ruc_iir_pixels_cache_NativePixelsCacheReader_get
  (JNIEnv *env, jclass cls, jlong blockId, jshort rowGroupId, jshort columnId)
{
    jbyteArray res;
    return res;
}


JNIEXPORT jbyteArray JNICALL Java_cn_edu_ruc_iir_pixels_cache_NativePixelsCacheReader_sch
  (JNIEnv *env, jclass cls, jlong blockId, jshort rowGroupId, jshort columnId)
{
    jbyteArray res;
    return res;
}