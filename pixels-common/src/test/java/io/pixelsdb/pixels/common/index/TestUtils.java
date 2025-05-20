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
package io.pixelsdb.pixels.common.index;
import java.lang.reflect.Field;

public class TestUtils
{
    // Set private field value
    public static void setPrivateField(Object target, String fieldName, Object value)
    {
        try
        {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true); // Allow access to private field
            field.set(target, value);
        }
        catch (NoSuchFieldException | IllegalAccessException e)
        {
            throw new RuntimeException("Failed to set private field", e);
        }
    }

    // Get private field value
    public static <T> T getPrivateField(Object target, String fieldName)
    {
        try
        {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true); // Allow access to private field
            return (T) field.get(target);
        }
        catch (NoSuchFieldException | IllegalAccessException e)
        {
            throw new RuntimeException("Failed to get private field", e);
        }
    }
}
