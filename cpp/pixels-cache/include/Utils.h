/*
 * Copyright 2021 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

 /*
 * @author hank
 */
#include <stdlib.h>
#include <jni.h>
#include "MemoryMappedFile.h"

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
