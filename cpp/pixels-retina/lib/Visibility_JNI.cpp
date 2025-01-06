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

#include "Visibility_JNI.h"
#include "Visibility.h"
#include <cstdlib>

/*
 * Class:     io_pixelsdb_pixels_retina_Visibility
 * Method:    destroyNativeObject
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_retina_Visibility_destroyNativeObject
        (JNIEnv *env, jobject obj, jlong nativeHandle) {
    Visibility* nativeObj = reinterpret_cast<Visibility*>(nativeHandle);
    delete nativeObj;
}

/*
 * Class:     io_pixelsdb_pixels_retina_Visibility
 * Method:    createNativeObject
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_io_pixelsdb_pixels_retina_Visibility_createNativeObject
        (JNIEnv *env, jobject obj) {
    Visibility* nativeObj = new Visibility();
    return reinterpret_cast<jlong>(nativeObj);
}

/*
 * Class:     io_pixelsdb_pixels_retina_Visibility
 * Method:    getVisibilityBitmap
 * Signature: (J[JJ)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_retina_Visibility_getVisibilityBitmap
        (JNIEnv *env, jobject obj, jlong epochTs, jlongArray bitmapArray, jlong nativeHandle) {
    Visibility* nativeObj = reinterpret_cast<Visibility*>(nativeHandle);
    std::uint64_t bitmap[BITMAP_ARRAY_SIZE];
    nativeObj->getVisibilityBitmap(static_cast<std::uint64_t>(epochTs), bitmap);
    env->SetLongArrayRegion(bitmapArray, 0, BITMAP_ARRAY_SIZE, reinterpret_cast<jlong*>(bitmap));
}

/*
 * Class:     io_pixelsdb_pixels_retina_Visibility
 * Method:    deleteRecord
 * Signature: (IJJ)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_retina_Visibility_deleteRecord
        (JNIEnv * env, jobject obj, jint rowId, jlong epochTs, jlong nativeHandle) {
    Visibility* nativeObj = reinterpret_cast<Visibility*>(nativeHandle);
    nativeObj->deleteRecord(static_cast<int>(rowId), static_cast<std::uint64_t>(epochTs));
}

/*
 * Class:     io_pixelsdb_pixels_retina_Visibility
 * Method:    createNewEpoch
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_retina_Visibility_createNewEpoch
        (JNIEnv *env, jobject obj, jlong epochTs, jlong nativeHandle) {
    Visibility* nativeObj = reinterpret_cast<Visibility*>(nativeHandle);
    nativeObj->createNewEpoch(static_cast<std::uint64_t>(epochTs));
}

/*
 * Class:     io_pixelsdb_pixels_retina_Visibility
 * Method:    cleanEpochArrAndPatchArr
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_retina_Visibility_cleanEpochArrAndPatchArr
        (JNIEnv *env, jobject obj, jlong cleanUpToEpochTs, jlong nativeHandle) {
    Visibility* nativeObj = reinterpret_cast<Visibility*>(nativeHandle);
    nativeObj->cleanEpochArrAndPatchArr(static_cast<std::uint64_t>(cleanUpToEpochTs));
}