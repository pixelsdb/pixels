#include "io_pixelsdb_pixels_cache_NativeDiskCacheContentReader.h"

JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_cache_NativeDiskCacheContentReader_read
  (JNIEnv* env, jobject jobj, jlong offset, jint length, jobject directBuf) {
  char* ret = (*env)->GetDirectBufferAddress(env, directBuf);
  



}