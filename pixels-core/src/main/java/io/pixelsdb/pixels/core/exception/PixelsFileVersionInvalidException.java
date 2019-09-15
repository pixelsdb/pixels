/*
 * Copyright 2017-2019 PixelsDB.
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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.exception;

import io.pixelsdb.pixels.core.PixelsVersion;

/**
 * @author guodong
 */
public class PixelsFileVersionInvalidException
        extends PixelsRuntimeException
{
    private static final long serialVersionUID = -6176420971384589629L;

    public PixelsFileVersionInvalidException(int version)
    {
        super(String.format("This is not a valid file version %d for current reader %d",
                version, PixelsVersion.currentVersion().getVersion()));
    }
}
