/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.planner.plan.physical;

/**
 * @author hank
 * @create 2023-09-19
 */
public enum ExchangeMethod
{
    batch, stream;

    /**
     * Case-insensitive parsing from String name to enum value.
     * @param name the name of exchanging method.
     * @return
     */
    public static ExchangeMethod from(String name)
    {
        return valueOf(name.toLowerCase());
    }

    /**
     * Whether the value is a valid exchanging method.
     * @param value
     * @return
     */
    public static boolean isValid(String value)
    {
        for (ExchangeMethod scheme : values())
        {
            if (scheme.equals(value))
            {
                return true;
            }
        }
        return false;
    }

    public boolean equals(String other)
    {
        return this.toString().equalsIgnoreCase(other);
    }

    public boolean equals(ExchangeMethod other)
    {
        // enums in Java can be compared using '=='.
        return this == other;
    }
}
