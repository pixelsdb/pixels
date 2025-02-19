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
#include "RGVisibilityJni.h"
#include "RGVisibility.h"
#include <stdexcept>

/*
 * Class:     io_pixelsdb_pixels_retina_RGVisibility
 * Method:    createNativeObject
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_io_pixelsdb_pixels_retina_RGVisibility_createNativeObject
  (JNIEnv* env, jobject, jlong rgRecordNum) {
    try {
        auto* rgVisibility = new RGVisibility(rgRecordNum);
        return reinterpret_cast<jlong>(rgVisibility);
    } catch (const std::exception& e) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), e.what());
        return 0;
    }
}

/*
 * Class:     io_pixelsdb_pixels_retina_RGVisibility
 * Method:    destroyNativeObject
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_retina_RGVisibility_destroyNativeObject
  (JNIEnv* env, jobject, jlong handle) {
    try {
        auto* rgVisibility = reinterpret_cast<RGVisibility*>(handle);
        delete rgVisibility;
    } catch (const std::exception& e) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), e.what());
    }
}

/*
 * Class:     io_pixelsdb_pixels_retina_RGVisibility
 * Method:    deleteRecord
 * Signature: (JJJ)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_retina_RGVisibility_deleteRecord
  (JNIEnv* env, jobject, jlong handle, jlong rowId, jlong timestamp) {
    try {
        auto* rgVisibility = reinterpret_cast<RGVisibility*>(handle);
        rgVisibility->deleteRGRecord(rowId, timestamp);
    } catch (const std::exception& e) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), e.what());
    }
}

/*
 * Class:     io_pixelsdb_pixels_retina_RGVisibility
 * Method:    getVisibilityBitmap
 * Signature: (JJ)[J
 */
JNIEXPORT jlongArray JNICALL Java_io_pixelsdb_pixels_retina_RGVisibility_getVisibilityBitmap
  (JNIEnv* env, jobject, jlong handle, jlong timestamp) {
    try {
        auto* rgVisibility = reinterpret_cast<RGVisibility*>(handle);
        auto* bitmap = rgVisibility->getRGVisibilityBitmap(timestamp);
        uint64_t bitmapSize = rgVisibility->getBitmapSize();
        jlongArray result = env->NewLongArray(bitmapSize);
        env->SetLongArrayRegion(result, 0, bitmapSize, reinterpret_cast<const jlong*>(bitmap));
        return result;
    } catch (const std::exception& e) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), e.what());
        return nullptr;
    }
}

/*
 * Class:     io_pixelsdb_pixels_retina_RGVisibility
 * Method:    garbageCollect
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_retina_RGVisibility_garbageCollect
  (JNIEnv* env, jobject, jlong handle, jlong timestamp) {
    try {
        auto* rgVisibility = reinterpret_cast<RGVisibility*>(handle);
        rgVisibility->collectRGGarbage(timestamp);
    } catch (const std::exception& e) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), e.what());
    }
}