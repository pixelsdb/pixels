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
#include "io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyHandle.h"
#include <rocksdb/db.h>
#include <cassert>

/**
 * @author Rolland1944
 * @create 2025-12-22
 */

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyHandle_disposeInternalJni(
    JNIEnv*,
    jclass,
    jlong jhandle)
{
    auto* cfh =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jhandle);

    assert(cfh != nullptr);
    delete cfh;
}