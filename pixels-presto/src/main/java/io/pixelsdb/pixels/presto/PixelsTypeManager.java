/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * refers to com.facebook.presto.spi.type.TestingTypeManager
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
        supportedTypes = ImmutableList.of(BOOLEAN, INTEGER, BIGINT, DOUBLE, REAL, VARCHAR, TIMESTAMP);
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