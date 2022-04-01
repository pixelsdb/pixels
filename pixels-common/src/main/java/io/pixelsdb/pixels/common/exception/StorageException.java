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

import java.io.IOException;

/**
 * Created at: 4/1/22
 * Author: hank
 */
public class StorageException extends IOException
{
    public StorageException()
    {
        super();
    }

    public StorageException(String message)
    {
        super(message);
    }

    public StorageException(Throwable cause)
    {
        super(cause);
    }

    public StorageException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
