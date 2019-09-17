/*
 * Copyright 2018-2019 PixelsDB.
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
#include <iostream>
#include "test_jni.h"
using namespace std;

static int sum = 0;

JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_cache_TestJni_sayHello(JNIEnv *env, jclass cls)
{
    cout << "Hello Native\n" << endl;
}

JNIEXPORT jint JNICALL Java_io_pixelsdb_pixels_cache_TestJni_add
  (JNIEnv *env, jclass cls, jint a)
{
    sum += a;
    return sum;
}

JNIEXPORT jbyteArray JNICALL Java_io_pixelsdb_pixels_cache_TestJni_echo
  (JNIEnv *env, jclass cls, jbyteArray bytes, jint length)
{
    jbyte* data = env->GetByteArrayElements(bytes, 0);
    jbyteArray res = env->NewByteArray(length);
    env->SetByteArrayRegion(res, 0, length, data);
    return res;
}