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

import io.pixelsdb.pixels.common.metadata.domain.Column;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.*;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This class is derived from com.facebook.presto.spi.type.TestingTypeManager
 *
 * We replace the loop lookup implementation of getType with a hashMap lookup.
 *
 * Created at: 19-6-1
 * Author: hank
 */
public class PixelsTypeManager
        implements TypeManager
{
    private static final PixelsTypeManager typeManager = new PixelsTypeManager();

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
        // Important: these are the data types we support int pixels-presto.
        // TODO: support more data types.
        // TODO: map presto data type to pixels data type more gracefully.
        // currently we do this by hard code in PixelsPageSource
        supportedTypes = ImmutableList.of(BOOLEAN, INTEGER, BIGINT, DOUBLE, REAL, VARCHAR, TIMESTAMP, DATE, TIME);
        signatureToType = new HashMap<>();
        for (Type type : supportedTypes)
        {
            signatureToType.put(type.getDisplayName(), type);
        }
    }

    private PixelsTypeManager () {}

    public Type getType(String signature)
    {
        return signatureToType.get(signature);
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        return signatureToType.get(signature.toString());
    }

    @Override
    public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        return getType(new TypeSignature(baseTypeName, typeParameters));
    }

    @Override
    public List<Type> getTypes()
    {
        return supportedTypes;
    }

    @Override
    public Collection<ParametricType> getParametricTypes()
    {
        return ImmutableList.of();
    }

    @Override
    public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
    {
        return false;
    }

    @Override
    public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
    {
        throw new UnsupportedOperationException();
    }
}