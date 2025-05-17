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
package io.pixelsdb.pixels.common.exception;

/**
 * @author lihaoyue
 * @create 2025-03-01
 */
public class IndexException extends Exception {
    public IndexException()
    {
        super();
    }

    public IndexException(String message)
    {
        super(message);
    }

    public IndexException(Throwable cause)
    {
        super(cause);
    }

    public IndexException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
