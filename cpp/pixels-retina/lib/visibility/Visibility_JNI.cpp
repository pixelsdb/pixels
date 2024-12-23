/*
 * Copyright 2024 PixelsDB.
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

#include "visibility/Visibility_JNI.h"
#include "visibility/Visibility.h"
#include <cstdlib>

/*
 * Class:     io_pixelsdb_pixels_core_deleter_DeleteTracker
 * Method:    createNativeObject
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_io_pixelsdb_pixels_core_deleter_DeleteTracker_createNativeObject
        (JNIEnv *env, jobject obj) {
    Visibility* nativeObj = new Visibility();
    return reinterpret_cast<jlong>(nativeObj);
}

/*
 * Class:     io_pixelsdb_pixels_core_deleter_DeleteTracker
 * Method:    destroyNativeObject
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_core_deleter_DeleteTracker_destroyNativeObject
(JNIEnv *env, jobject obj, jlong nativeHandle) {
    Visibility* nativeObj = reinterpret_cast<Visibility*>(nativeHandle);
    delete nativeObj;
}

/*
 * Class:     io_pixelsdb_pixels_core_deleter_DeleteTracker
 * Method:    getReadableBitmap
 * Signature: (IJ)[J
 */
JNIEXPORT jlongArray JNICALL Java_io_pixelsdb_pixels_core_deleter_DeleteTracker_getReadableBitmap
        (JNIEnv *env, jobject obj, jint timestamp, jlong nativeHandle) {
    Visibility* nativeObj = reinterpret_cast<Visibility*>(nativeHandle);
    std::vector<uint64_t> bitmap = nativeObj->getReadableBitmap(static_cast<int>(timestamp));

    jlongArray result = env->NewLongArray(bitmap.size());
    if (result == NULL) {
        return NULL;
    }
    env->SetLongArrayRegion(result, 0, bitmap.size(), reinterpret_cast<jlong*>(bitmap.data()));
    return result;
}

/*
 * Class:     io_pixelsdb_pixels_core_deleter_DeleteTracker
 * Method:    deleteRow
 * Signature: (IIJ)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_core_deleter_DeleteTracker_deleteRow
(JNIEnv *env, jobject obj, jint timestamp, jint rowId, jlong nativeHandle) {
    Visibility* nativeObj = reinterpret_cast<Visibility*>(nativeHandle);
    nativeObj->deleteRow(static_cast<int>(timestamp), static_cast<int>(rowId));
}