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

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.metadata.domain.Column;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Parse Pixels Column to Presto Type.
 *
 * Created at: 19-6-1
 * Author: hank
 */
public class PixelsTypeParser
{
    private static final PixelsTypeParser typeManager = new PixelsTypeParser();

    public static Type getColumnType (Column column)
    {
        return typeManager.getType(column.getType());
    }

    /**
     * refers to org.apache.carbondata.presto.Types
     */
    public static <A, B extends A> B checkObjectType(A value, Class<B> target, String name)
    {
        requireNonNull(value, String.format(Locale.ENGLISH, "%s is null", name));
        checkArgument(target.isInstance(value), "%s must be of type %s, not %s", name, target.getName(),
                value.getClass().getName());
        return target.cast(value);
    }

    private static final Map<String, Type> signatureToType;
    private static final List<Type> supportedTypes;

    static
    {
        // Important: these are the data types we support in pixels-presto.
        // The types supported here should be consistent with the switch..case in PixelsPageSource.
        supportedTypes = ImmutableList.of(
                BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT,
                DOUBLE, REAL, VARCHAR, TIMESTAMP, DATE, TIME);
        signatureToType = new HashMap<>();
        for (Type type : supportedTypes)
        {
            /**
             * Issue #163:
             * Register the lowercase of the type's display name.
             */
            signatureToType.put(type.getDisplayName().toLowerCase(), type);
        }
        /**
         * Issue #163:
         * Support char(n) using varchar.
         */
        signatureToType.put("char", VARCHAR);
    }

    private PixelsTypeParser() {}

    public Type getType(String signature)
    {
        /**
         * Issue #163:
         * The signature in signatureToType is the prefix in lowercase of the type's display name.
         * Fore example, varchar or char instead of varchar(10) or char(10). Therefore we use the
         * lowercase of the prefix of the parameter to do the matching.
         */
        int index = signature.indexOf("(");
        if (index > 0)
        {
            signature = signature.substring(0, index);
        }
        return signatureToType.get(signature.toLowerCase());
    }
}