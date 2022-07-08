/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.executor.aggregation;

import java.util.HashSet;
import java.util.Set;

/**
 * The aggregate function type
 * @author hank
 * @date 04/07/2022
 */
public enum FunctionType
{
    UNKNOWN, // The first enum value is the default value.
    SUM,
    MIN,
    MAX,
    COUNT;

    private static final Set<String> supported;

    static
    {
        supported = new HashSet<>();
        supported.add(SUM.name());
        supported.add(MIN.name());
        supported.add(MAX.name());
        supported.add(COUNT.name());
    }

    public static FunctionType fromName(String name)
    {
        return valueOf(name.toUpperCase());
    }

    public static boolean isSupported(String name)
    {
        return supported.contains(name.toUpperCase());
    }
}
