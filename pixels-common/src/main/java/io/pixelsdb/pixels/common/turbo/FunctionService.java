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
package io.pixelsdb.pixels.common.turbo;

/**
 * The cloud function services we support.
 * Modify this enum if we add new cloud function service support.
 *
 * @create 2023-04-10
 * @author hank
 */
public enum FunctionService
{
    lambda,  // AWS Lambda
    vhive,   // vhive
    spike,    // Spike
    ;

    /**
     * Case-insensitive parsing from String name to enum value.
     * @param name the name of cloud function service.
     * @return
     */
    public static FunctionService from(String name)
    {
        return valueOf(name.toLowerCase());
    }

    /**
     * Whether the value is a valid cloud function service.
     * @param value
     * @return
     */
    public static boolean isValid(String value)
    {
        for (FunctionService scheme : values())
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

    public boolean equals(FunctionService other)
    {
        // enums in Java can be compared using '=='.
        return this == other;
    }
}
