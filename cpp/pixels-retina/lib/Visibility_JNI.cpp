/*
 * Copyright 2025 PixelsDB.
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

#include "Visibility.h"
#include "Visibility_JNI.h"

#include <cstdlib>
#include <stdexcept>

/*
 * Class:     io_pixelsdb_pixels_retina_Visibility
 * Method:    createNativeObject
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_retina_Visibility_createNativeObject__(JNIEnv *env,
                                                               jobject obj) {
    Visibility *nativeObj = new Visibility();
    return reinterpret_cast<jlong>(nativeObj);
}

/*
 * Class:     io_pixelsdb_pixels_retina_Visibility
 * Method:    createNativeObject
 * Signature: (J[J)J
 */
JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_retina_Visibility_createNativeObject__J_3J(
    JNIEnv *env, jobject obj, jlong timestamp, jlongArray bitmapArray) {
    jsize length = env->GetArrayLength(bitmapArray);
    if (length != 4) {
        throw std::invalid_argument("Bitmap array must have 4 elements");
    }
    jlong *bitmapArrayElements = env->GetLongArrayElements(bitmapArray, nullptr);
    if (bitmapArrayElements == nullptr) {
        throw std::runtime_error("Failed to get bitmap array elements");
    }

    std::uint64_t bitmap[4];
    for (int i = 0; i < 4; i++) {
        bitmap[i] = static_cast<std::uint64_t>(bitmapArrayElements[i]);
    }

    Visibility *nativeObj =
        new Visibility(static_cast<std::uint64_t>(timestamp),
                       bitmap);

    env->ReleaseLongArrayElements(bitmapArray, bitmapArrayElements, JNI_ABORT);
    return reinterpret_cast<jlong>(nativeObj);
}

/*
 * Class:     io_pixelsdb_pixels_retina_Visibility
 * Method:    destroyNativeObject
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_retina_Visibility_destroyNativeObject(JNIEnv *env, jobject obj,
                                                              jlong nativeHandle) {
    Visibility *nativeObj = reinterpret_cast<Visibility *>(nativeHandle);
    delete nativeObj;
}

/*
 * Class:     io_pixelsdb_pixels_retina_Visibility
 * Method:    getVisibilityBitmap
 * Signature: (J[JJ)V
 */
JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_retina_Visibility_getVisibilityBitmap(JNIEnv *env, jobject obj,
                                                              jlong timestamp, jlongArray bitmapArray,
                                                              jlong nativeHandle) {
    Visibility *nativeObj = reinterpret_cast<Visibility *>(nativeHandle);
    std::uint64_t bitmap[4];
    nativeObj->getVisibilityBitmap(static_cast<std::uint64_t>(timestamp), bitmap);
    env->SetLongArrayRegion(bitmapArray, 0, 4,
                            reinterpret_cast<jlong *>(bitmap));
}

/*
 * Class:     io_pixelsdb_pixels_retina_Visibility
 * Method:    deleteRecord
 * Signature: (IJJ)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_retina_Visibility_deleteRecord(
    JNIEnv *env, jobject obj, jint rowId, jlong timestamp, jlong nativeHandle) {
    if (rowId < 0 || rowId > 255) {
        throw std::invalid_argument("Row ID must be between 0 and 255");
    }
    Visibility *nativeObj = reinterpret_cast<Visibility *>(nativeHandle);
    nativeObj->deleteRecord(static_cast<uint8_t>(rowId),
                            static_cast<std::uint64_t>(timestamp));
};
