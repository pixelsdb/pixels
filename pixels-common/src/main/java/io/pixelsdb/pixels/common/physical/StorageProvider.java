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
package io.pixelsdb.pixels.common.physical;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Created at: 2023-04-15
 * Author: hank
 */
public interface StorageProvider
{
    /**
     * Create an instance of the file/object storage system handler.
     */
    Storage createStorage(@Nonnull Storage.Scheme scheme) throws IOException;

    /**
     * @param scheme the given storage scheme.
     * @return true if this physical reader provider is compatible with the given storage scheme.
     */
    boolean compatibleWith(@Nonnull Storage.Scheme scheme);
}
