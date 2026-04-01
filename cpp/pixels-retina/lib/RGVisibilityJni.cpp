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

#include "RetinaBase.h"
#include "RGVisibilityJni.h"
#include "RGVisibility.h"
#include <stdexcept>

/*
 * Class:     io_pixelsdb_pixels_retina_RGVisibility
 * Method:    createNativeObject
 * Signature: (JJ[J)J
 *
 * Converts the Java bitmap array to a native vector when present, then
 * forwards to a single RGVisibility constructor call.
 */
JNIEXPORT jlong JNICALL Java_io_pixelsdb_pixels_retina_RGVisibility_createNativeObject
  (JNIEnv* env, jobject, jlong rgRecordNum, jlong timestamp, jlongArray bitmap) {
    try {
        std::vector<uint64_t> bitmapData;
        const std::vector<uint64_t>* bitmapPtr = nullptr;
        if (bitmap != nullptr) {
            jsize len = env->GetArrayLength(bitmap);
            jlong* body = env->GetLongArrayElements(bitmap, nullptr);
            bitmapData.assign(reinterpret_cast<uint64_t*>(body),
                              reinterpret_cast<uint64_t*>(body) + len);
            env->ReleaseLongArrayElements(bitmap, body, JNI_ABORT);
            bitmapPtr = &bitmapData;
        }
        return reinterpret_cast<jlong>(new RGVisibilityInstance(
            static_cast<uint64_t>(rgRecordNum),
            static_cast<uint64_t>(timestamp),
            bitmapPtr));
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
        auto* rgVisibility = reinterpret_cast<RGVisibilityInstance*>(handle);
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
  (JNIEnv* env, jobject, jint rowId, jlong timestamp, jlong handle) {
    try {
        auto* rgVisibility = reinterpret_cast<RGVisibilityInstance*>(handle);
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
  (JNIEnv* env, jobject, jlong timestamp, jlong handle) {
    uint64_t* bitmap = nullptr;
    uint64_t bitmapSize = 0;
    try {
        auto* rgVisibility = reinterpret_cast<RGVisibilityInstance*>(handle);
        bitmap = rgVisibility->getRGVisibilityBitmap(timestamp);
        bitmapSize = rgVisibility->getBitmapSize();
        jlongArray result = env->NewLongArray(bitmapSize);
        env->SetLongArrayRegion(result, 0, bitmapSize, reinterpret_cast<const jlong*>(bitmap));
        size_t byteSize = bitmapSize * sizeof(uint64_t);
        pixels::g_retina_tracked_memory.fetch_sub(byteSize, std::memory_order_relaxed);
        delete[] bitmap;
        return result;
    } catch (const std::exception& e) {
        if (bitmap) {
            size_t byteSize = bitmapSize * sizeof(uint64_t);
            pixels::g_retina_tracked_memory.fetch_sub(byteSize, std::memory_order_relaxed);
            delete[] bitmap;
        }
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), e.what());
        return nullptr;
    }
}

/*
 * Class:     io_pixelsdb_pixels_retina_RGVisibility
 * Method:    garbageCollect
 * Signature: (JJ)[J
 */
JNIEXPORT jlongArray JNICALL Java_io_pixelsdb_pixels_retina_RGVisibility_garbageCollect
  (JNIEnv* env, jobject, jlong timestamp, jlong handle) {
    try {
        auto* rgVisibility = reinterpret_cast<RGVisibilityInstance*>(handle);
        std::vector<uint64_t> snapshot = rgVisibility->collectRGGarbage(timestamp);
        jlongArray result = env->NewLongArray(snapshot.size());
        env->SetLongArrayRegion(result, 0, snapshot.size(),
            reinterpret_cast<const jlong*>(snapshot.data()));
        return result;
    } catch (const std::exception& e) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), e.what());
        return nullptr;
    }
}

/*
 * Class:     io_pixelsdb_pixels_retina_RGVisibility
 * Method:    exportChainItemsAfter
 * Signature: (JJ)[J
 */
JNIEXPORT jlongArray JNICALL Java_io_pixelsdb_pixels_retina_RGVisibility_exportChainItemsAfter
  (JNIEnv* env, jobject, jlong safeGcTs, jlong handle) {
    try {
        auto* rgVis = reinterpret_cast<RGVisibilityInstance*>(handle);
        std::vector<uint64_t> items = rgVis->exportChainItemsAfter(static_cast<uint64_t>(safeGcTs));
        jlongArray result = env->NewLongArray(items.size());
        env->SetLongArrayRegion(result, 0, items.size(), reinterpret_cast<const jlong*>(items.data()));
        return result;
    } catch (const std::exception& e) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), e.what());
        return nullptr;
    }
}

/*
 * Class:     io_pixelsdb_pixels_retina_RGVisibility
 * Method:    importDeletionChain
 * Signature: ([JJ)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_retina_RGVisibility_importDeletionChain
  (JNIEnv* env, jobject, jlongArray items, jlong handle) {
    try {
        auto* rgVis = reinterpret_cast<RGVisibilityInstance*>(handle);
        jsize len = env->GetArrayLength(items);
        jlong* body = env->GetLongArrayElements(items, nullptr);
        rgVis->importDeletionChain(reinterpret_cast<const uint64_t*>(body), len / 2);
        env->ReleaseLongArrayElements(items, body, JNI_ABORT);
    } catch (const std::exception& e) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), e.what());
    }
}

/*
 * Class:     io_pixelsdb_pixels_retina_RGVisibility
 * Method:    getNativeMemoryUsage
 * Returns the total bytes currently allocated by the process as tracked by jemalloc.
 */
JNIEXPORT jlong JNICALL Java_io_pixelsdb_pixels_retina_RGVisibility_getNativeMemoryUsage
  (JNIEnv* env, jclass) {
#ifdef ENABLE_JEMALLOC
    size_t allocated = 0;
    size_t sz = sizeof(size_t);
    uint64_t epoch = 1;

    // 1. Try to refresh jemalloc epoch to ensure stats are current.
    // Return -2 if this fails, as defined in Java's handleMemoryMetric.
    if (je_mallctl("epoch", NULL, NULL, &epoch, sizeof(uint64_t)) != 0) {
        return -2;
    }

    // 2. Try to read the actual allocated bytes.
    // Return -3 if this fails, which often implies a config/prefix mismatch.
    if (je_mallctl("stats.allocated", &allocated, &sz, NULL, 0) != 0) {
        return -3;
    }

    // Success: return the positive value
    return static_cast<jlong>(allocated);
#else
    // -1 triggers the "monitoring is disabled" message in Java
    return -1;
#endif
}

/*
 * Class:     io_pixelsdb_pixels_retina_RGVisibility
 * Method:    getRetinaTrackedMemoryUsage
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_io_pixelsdb_pixels_retina_RGVisibility_getRetinaTrackedMemoryUsage
  (JNIEnv *env, jclass clazz) {
    // Read the current value from the atomic counter using relaxed memory order
    // as this is a simple statistic and doesn't require strict synchronization.
    return static_cast<jlong>(pixels::g_retina_tracked_memory.load(std::memory_order_relaxed));
}

/*
 * Implementation for tracking the number of active objects.
 */
JNIEXPORT jlong JNICALL Java_io_pixelsdb_pixels_retina_RGVisibility_getRetinaObjectCount
  (JNIEnv *env, jclass clazz) {
    return static_cast<jlong>(pixels::g_retina_object_count.load(std::memory_order_relaxed));
}
