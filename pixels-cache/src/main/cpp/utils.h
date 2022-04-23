#include "stdlib.h"
#include "jni.h"
#include "memory_mapped_file.h"

#ifndef _Included_utils
#define _Included_utils

MemoryMappedFile build_mmap_f(JNIEnv *env, jobject obj, const char* field)
{
  jfieldID fid; /* store the field ID */
  jclass cls = (*env)->GetObjectClass(env, obj);
  /* Look for the instance field s in cls */
  fid = (*env)->GetFieldID(env, cls, field, "Lio/pixelsdb/pixels/cache/MemoryMappedFile;");
  if (fid == NULL)
  {
    return (MemoryMappedFile){NULL, -1}; /* failed to find the field */
  }
  jobject index_file = (*env)->GetObjectField(env, obj, fid);
  jclass mmap_file = (*env)->GetObjectClass(env, index_file);
  // parse file location
  fid = (*env)->GetFieldID(env, mmap_file, "addr", "J");
  jlong addr = (*env)->GetLongField(env, index_file, fid);
  fid = (*env)->GetFieldID(env, mmap_file, "size", "J");
  jlong size = (*env)->GetLongField(env, index_file, fid);

  return (MemoryMappedFile){(char *)addr, size};
}

#endif
