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
package io.pixelsdb.pixels.core.encoding;

import io.pixelsdb.pixels.common.exception.InvalidArgumentException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Higher encoding level may have better compression ratio but higher computation overhead.
 * @author hank
 * @create 2023-08-12
 */
public enum EncodingLevel
{
    EL0(0), EL1(1), EL2(2);

    private final int level;

    EncodingLevel (int level)
    {
        this.level = level;
    }

    public static EncodingLevel from (int level)
    {
        switch (level)
        {
            case 0:
                return EL0;
            case 1:
                return EL1;
            case 2:
                return EL2;
            default:
                throw new InvalidArgumentException("invalid encoding level " + level);
        }
    }

    public static EncodingLevel from (String level)
    {
        requireNonNull(level, "level is null");
        return from(Integer.parseInt(level));
    }

    public static boolean isValid(int level)
    {
        return level >= 0 && level <= 2;
    }

    public boolean ge(int level)
    {
        checkArgument(isValid(level), "leve is invalid");
        return this.level >= level;
    }

    public boolean ge(EncodingLevel encodingLevel)
    {
        requireNonNull(level, "level is null");
        return this.level >= encodingLevel.level;
    }

    public boolean equals(int level)
    {
        return this.level == level;
    }

    public boolean equals(EncodingLevel other)
    {
        // enums in Java can be compared using '=='.
        return this == other;
    }
}
