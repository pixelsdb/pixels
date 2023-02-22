/*
 * Copyright 2019-2022 PixelsDB.
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
package io.pixelsdb.pixels.common.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

/**
 * @author hank
 * @date 8/27/22
 */
public final class JvmUtils
{
    public static final Unsafe unsafe;
    public static final ByteOrder nativeOrder;

    public static final boolean nativeIsLittleEndian;

    static
    {
        try
        {
            Field singleOneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
            singleOneInstanceField.setAccessible(true);
            unsafe = (Unsafe) singleOneInstanceField.get(null);
            nativeOrder = ByteOrder.nativeOrder();
            nativeIsLittleEndian = (nativeOrder == ByteOrder.LITTLE_ENDIAN);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
