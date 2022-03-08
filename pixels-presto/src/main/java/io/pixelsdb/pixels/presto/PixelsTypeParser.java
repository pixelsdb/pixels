/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.presto;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.inject.Inject;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;

import static java.util.Objects.requireNonNull;

/**
 * Parse column type signature to Presto or Pixels data type.
 *
 * Created at: 19-6-1
 * Author: hank
 */
public class PixelsTypeParser
{
    private final TypeManager baseRegistry;

    @Inject
    public PixelsTypeParser(TypeManager typeRegistry)
    {
        this.baseRegistry = requireNonNull(typeRegistry, "typeRegistry is null");
    }

    public TypeDescription parsePixelsType(String signature)
    {
        try
        {
            return TypeDescription.fromString(signature);
        }
        catch (RuntimeException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_DATA_TYPE_ERROR,
                    "failed to parse Pixels data type '" + signature + "'", e);
        }

    }

    public Type parsePrestoType(String signature)
    {
        try
        {
            return this.baseRegistry.getType(TypeSignature.parseTypeSignature(signature));
        }
        catch (RuntimeException e)
        {
            throw new PrestoException(PixelsErrorCode.PIXELS_DATA_TYPE_ERROR,
                    "failed to parse Presto data type '" + signature + "'", e);
        }
    }
}