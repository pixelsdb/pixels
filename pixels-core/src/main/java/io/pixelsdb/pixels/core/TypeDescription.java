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
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core;

import com.google.common.collect.ImmutableSet;
import io.pixelsdb.pixels.core.utils.Decimal;
import io.pixelsdb.pixels.core.vector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * TypeDescription derived from org.apache.orc
 * <p>
 * Schema description in a Pixels file.
 */
public final class TypeDescription implements Comparable<TypeDescription>, Serializable, Cloneable
{
    private static final long serialVersionUID = 4270695889340023552L;
    /**
     * In SQL standard, the max precision of decimal is 38.
     * Issue #196: support short decimal of which the max precision is 18.
     * Issue #203: support long decimal of which the max precision is 38.
     */
    public static final int MAX_SHORT_DECIMAL_PRECISION = 18;
    public static final int MAX_LONG_DECIMAL_PRECISION = 38;
    /**
     * In SQL standard, the max scale of decimal is 38.
     * Issue #196: support short decimal of which the max scale is 18.
     * Issue #203: support long decimal of which the max scale is 38.
     */
    public static final int MAX_SHORT_DECIMAL_SCALE = 18;
    public static final int MAX_LONG_DECIMAL_SCALE = 38;
    /**
     * In SQL standard, the default precision of decimal is 38.
     * Issue #196: support short decimal of which the default precision is 18.
     * Issue #203: support long decimal of which the default precision is 38.
     */
    public static final int DEFAULT_SHORT_DECIMAL_PRECISION = 18;
    public static final int DEFAULT_LONG_DECIMAL_PRECISION = 38;
    /**
     * In SQL standard, the default scale of decimal is 0.
     */
    public static final int DEFAULT_DECIMAL_SCALE = 0;
    /**
     * The default length of varchar, binary, and varbinary.
     */
    public static final int DEFAULT_VARCHAR_OR_BINARY_LENGTH = 65535;
    /**
     * The default dimension of vector
     */
    public static final int DEFAULT_VECTOR_DIMENSION = 2;
    /**
     * the supported maximum dimension
     */
    public static final int MAXIMUM_VECTOR_DIMENSION = 4096;
    /**
     * It is a standard that the default length of char is 1.
     */
    public static final int DEFAULT_CHAR_LENGTH = 1;
    /**
     * In SQL standard, the default precision of timestamp is 6 (i.e., microseconds), however,
     * in Pixels, we use the default precision 3 (i.e., milliseconds), which is consistent with Trino.
     */
    public static final int DEFAULT_TIMESTAMP_PRECISION = 3;
    /**
     * In SQL standard, the default precision of time is 6 (i.e., microseconds), however,
     * in Pixels, we use the default precision 3 (i.e., milliseconds), which is consistent with Trino.
     */
    public static final int DEFAULT_TIME_PRECISION = 3;
    /**
     * 9 = nanosecond, 6 = microsecond, 3 = millisecond, 0 = second.
     * <p>Although 64-bit long is enough to encode the nanoseconds for now, it is
     * risky to encode a nanosecond time in the near future, thus some query engines
     * such as Trino only use long to encode a timestamp up to 6 precision.
     * Therefore, we also set the max precision of long encoded timestamp to 6 in Pixels.</p>
     */
    public static final int MAX_TIMESTAMP_PRECISION = 6;
    /**
     * 9 = nanosecond, 6 = microsecond, 3 = millisecond, 0 = second.
     * <p>In Pixels, we use 32-bit integer to store time, thus we can support time precision up to 4.
     * For simplicity and compatibility to {@link java.sql.Time}, we further limit the precision to 3.</p>
     */
    public static final int MAX_TIME_PRECISION = 3;

    /**
     * The type of the hidden commit timestamp column.
     */
    public static final TypeDescription HIDDEN_COLUMN_TYPE = new TypeDescription(TypeDescription.Category.LONG);
    /**
     * The name of the hidden commit timestamp column.
     */
    public static final String HIDDEN_COLUMN_NAME = "hidden_commit_timestamp";


    private static final Pattern UNQUOTED_NAMES = Pattern.compile("^\\w+$");

    private static final Logger logger = LogManager.getLogger(TypeDescription.class);

    @Override
    public int compareTo(TypeDescription other)
    {
        if (this == other)
        {
            return 0;
        }
        else if (other == null)
        {
            return -1;
        }
        else
        {
            int result = category.compareTo(other.category);
            if (result == 0)
            {
                switch (category)
                {
                    case BINARY:
                    case VARBINARY:
                    case CHAR:
                    case VARCHAR:
                        result = maxLength - other.maxLength;
                        break;
                    case TIME:
                    case TIMESTAMP:
                        result = precision - other.precision;
                        break;
                    case DECIMAL:
                        result = precision - other.precision;
                        if (result == 0)
                        {
                            result = scale - other.scale;
                        }
                        break;
                    case STRUCT:
                        if (children.size() != other.children.size())
                        {
                            return children.size() - other.children.size();
                        }
                        for (int c = 0; result == 0 && c < children.size(); ++c)
                        {
                            result = fieldNames.get(c).compareTo(other.fieldNames.get(c));
                            if (result == 0)
                            {
                                result = children.get(c).compareTo(other.children.get(c));
                            }
                        }
                        break;
                    default:
                        // PASS
                }
            }
            return result;
        }
    }

    public enum Category
    {
        /*
         * Issue #196:
         * Support alias of data types.
         *
         * Issue #170:
         * Add external and internal java types.
         */
        BOOLEAN(true, boolean.class, byte.class, "boolean"),
        BYTE(true, byte.class, byte.class, "tinyint", "byte"),
        SHORT(true, short.class, long.class, "smallint", "short"),
        INT(true, int.class, long.class, "integer", "int"),
        LONG(true, long.class, long.class, "bigint", "long"),
        FLOAT(true, float.class, long.class, "float", "real"),
        DOUBLE(true, double.class, long.class, "double"),
        DECIMAL(true, double.class, Decimal.class, "decimal"),
        STRING(true, String.class, byte[].class, "string"),
        DATE(true, Date.class, int.class, "date"),
        TIME(true, Time.class, int.class, "time"),
        TIMESTAMP(true, Timestamp.class, long.class, "timestamp"),
        VARBINARY(true, byte[].class, byte[].class, "varbinary"),
        BINARY(true, byte[].class, byte[].class, "binary"),
        VARCHAR(true, byte[].class, byte[].class,"varchar"),
        CHAR(true, byte[].class, byte[].class,"char"),
        STRUCT(false, Class.class, Class.class, "struct"),
        VECTOR(false, double[].class, double[].class, "vector", "array");

        /**
         * Ensure that all elements in names are in <b>lowercase</b>.
         * @param isPrimitive
         * @param names
         */
        Category(boolean isPrimitive, Class<?> externalJavaType, Class<?> internalJavaType, String... names)
        {
            checkArgument(names != null && names.length > 0,
                    "names is null or empty");
            this.primaryName = names[0];
            this.isPrimitive = isPrimitive;
            this.allNames.addAll(Arrays.asList(names));
            this.externalJavaType = externalJavaType;
            this.internalJavaType = internalJavaType;
        }

        final boolean isPrimitive;
        final String primaryName;
        final Set<String> allNames = new HashSet<>();
        final Class<?> externalJavaType;
        final Class<?> internalJavaType;

        public boolean isPrimitive()
        {
            return isPrimitive;
        }

        public String getPrimaryName()
        {
            return primaryName;
        }

        public Set<String> getAllNames()
        {
            return ImmutableSet.copyOf(this.allNames);
        }

        public Class<?> getExternalJavaType()
        {
            return externalJavaType;
        }

        public Class<?> getInternalJavaType()
        {
            return internalJavaType;
        }

        public boolean match(String name)
        {
            return this.allNames.contains(name.toLowerCase(Locale.ENGLISH));
        }
    }

    public static TypeDescription createBoolean()
    {
        return new TypeDescription(Category.BOOLEAN);
    }

    public static TypeDescription createByte()
    {
        return new TypeDescription(Category.BYTE);
    }

    public static TypeDescription createShort()
    {
        return new TypeDescription(Category.SHORT);
    }

    public static TypeDescription createInt()
    {
        return new TypeDescription(Category.INT);
    }

    public static TypeDescription createLong()
    {
        return new TypeDescription(Category.LONG);
    }

    public static TypeDescription createFloat()
    {
        return new TypeDescription(Category.FLOAT);
    }

    public static TypeDescription createDouble()
    {
        return new TypeDescription(Category.DOUBLE);
    }

    public static TypeDescription createDecimal(int precision, int scale)
    {
        TypeDescription type = new TypeDescription(Category.DECIMAL);
        type.withPrecision(precision);
        type.withScale(scale);
        return type;
    }

    public static TypeDescription createString()
    {
        return new TypeDescription(Category.STRING);
    }

    public static TypeDescription createDate()
    {
        return new TypeDescription(Category.DATE);
    }

    public static TypeDescription createTime(int precision)
    {
        TypeDescription type = new TypeDescription(Category.TIME);
        type.withPrecision(precision);
        return type;

    }

    public static TypeDescription createTimestamp(int precision)
    {
        TypeDescription type = new TypeDescription(Category.TIMESTAMP);
        type.withPrecision(precision);
        return type;
    }

    public static TypeDescription createVarbinary(int maxLength)
    {
        TypeDescription type = new TypeDescription(Category.VARBINARY);
        type.withMaxLength(maxLength);
        return type;
    }

    public static TypeDescription createBinary(int maxLength)
    {
        TypeDescription type = new TypeDescription(Category.BINARY);
        type.withMaxLength(maxLength);
        return type;
    }

    public static TypeDescription createVarchar(int maxLength)
    {
        TypeDescription type = new TypeDescription(Category.VARCHAR);
        type.withMaxLength(maxLength);
        return type;
    }

    public static TypeDescription createChar(int maxLength)
    {
        TypeDescription type = new TypeDescription(Category.CHAR);
        type.withMaxLength(maxLength);
        return type;
    }

    public static TypeDescription createStruct()
    {
        return new TypeDescription(Category.STRUCT);
    }

    public static TypeDescription createVector(int dimension)
    {
        TypeDescription type = new TypeDescription(Category.VECTOR);
        type.withDimension(dimension);
        return type;
    }

    public static TypeDescription createSchema(List<PixelsProto.Type> types)
    {
        TypeDescription schema = TypeDescription.createStruct();
        for (PixelsProto.Type type : types)
        {
            String fieldName = type.getName();
            TypeDescription fieldType;
            switch (type.getKind())
            {
                case BOOLEAN:
                    fieldType = TypeDescription.createBoolean();
                    break;
                case LONG:
                    fieldType = TypeDescription.createLong();
                    break;
                case INT:
                    fieldType = TypeDescription.createInt();
                    break;
                case SHORT:
                    fieldType = TypeDescription.createShort();
                    break;
                case BYTE:
                    fieldType = TypeDescription.createByte();
                    break;
                case FLOAT:
                    fieldType = TypeDescription.createFloat();
                    break;
                case DOUBLE:
                    fieldType = TypeDescription.createDouble();
                    break;
                case DECIMAL:
                    fieldType = TypeDescription.createDecimal(
                            type.getPrecision(), type.getScale());
                    break;
                case VARCHAR:
                    fieldType = TypeDescription.createVarchar(
                            type.getMaximumLength());
                    break;
                case CHAR:
                    fieldType = TypeDescription.createChar(
                            type.getMaximumLength());
                    break;
                case STRING:
                    fieldType = TypeDescription.createString();
                    break;
                case DATE:
                    fieldType = TypeDescription.createDate();
                    break;
                case TIME:
                    fieldType = TypeDescription.createTime(
                            type.getPrecision());
                    break;
                case TIMESTAMP:
                    fieldType = TypeDescription.createTimestamp(
                            type.getPrecision());
                    break;
                case VARBINARY:
                    fieldType = TypeDescription.createVarbinary(
                            type.getMaximumLength());
                    break;
                case BINARY:
                    fieldType = TypeDescription.createBinary(
                            type.getMaximumLength());
                    break;
                case VECTOR:
                    fieldType = TypeDescription.createVector(
                            type.getDimension()
                    );
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type: " +
                            type.getKind());
            }
            schema.addField(fieldName, fieldType);
        }
        return schema;
    }

    /**
     * Based on the column type, create the corresponding schema.
     * Column types are represented as string types, e.g. "decimal(15, 2)", "varchar(10)".
     * @param typeNames
     * @return
     */
    public static TypeDescription createSchemaFromStrings(List<String> typeNames)
    {
        TypeDescription schema = TypeDescription.createStruct();
        for (String typeName : typeNames)
        {
            typeName = typeName.trim().toLowerCase();
            TypeDescription fieldType;

            if (typeName.startsWith("decimal"))
            {
                Matcher matcher = Pattern.compile("decimal\\((\\d+),(\\d+)\\)").matcher(typeName);
                if (matcher.matches())
                {
                    int precision = Integer.parseInt(matcher.group(1));
                    int scale = Integer.parseInt(matcher.group(2));
                    fieldType = TypeDescription.createDecimal(precision, scale);
                } else
                {
                    throw new IllegalArgumentException("Unknown type: " + typeName);
                }
            } else if (typeName.startsWith("varchar"))
            {
                Matcher matchar = Pattern.compile("varchar\\((\\d+)\\)").matcher(typeName);
                if (matchar.matches())
                {
                    int maxLength = Integer.parseInt(matchar.group(1));
                    fieldType = TypeDescription.createVarchar(maxLength);
                } else
                {
                    throw new IllegalArgumentException("Unknown type: " + typeName);
                }
            } else if (typeName.startsWith("char"))
            {
                Matcher matcher = Pattern.compile("char\\((\\d+)\\)").matcher(typeName);
                if (matcher.matches())
                {
                    int maxLength = Integer.parseInt(matcher.group(1));
                    fieldType = TypeDescription.createChar(maxLength);
                } else
                {
                    throw new IllegalArgumentException("Unknown type: " + typeName);
                }
            } else if (typeName.startsWith("varbinary"))
            {
                Matcher matcher = Pattern.compile("varbinary\\((\\d+)\\)").matcher(typeName);
                if (matcher.matches())
                {
                    int maxLength = Integer.parseInt(matcher.group(1));
                    fieldType = TypeDescription.createVarbinary(maxLength);
                } else
                {
                    throw new IllegalArgumentException("Unknown type: " + typeName);
                }
            } else if (typeName.startsWith("binary"))
            {
                Matcher matcher = Pattern.compile("binary\\((\\d+)\\)").matcher(typeName);
                if (matcher.matches())
                {
                    int maxLength = Integer.parseInt(matcher.group(1));
                    fieldType = TypeDescription.createBinary(maxLength);
                } else
                {
                    throw new IllegalArgumentException("Unknown type: " + typeName);
                }
            } else if (typeName.startsWith("time"))
            {
                Matcher matcher = Pattern.compile("time\\((\\d+)\\)").matcher(typeName);
                if (matcher.matches())
                {
                    int precision = Integer.parseInt(matcher.group(1));
                    fieldType = TypeDescription.createTime(precision);
                } else
                {
                    throw new IllegalArgumentException("Unknown type: " + typeName);
                }
            } else if (typeName.startsWith("timestamp"))
            {
                Matcher matcher = Pattern.compile("timestamp\\((\\d+)\\)").matcher(typeName);
                if (matcher.matches())
                {
                    int precision = Integer.parseInt(matcher.group(1));
                    fieldType = TypeDescription.createTimestamp(precision);
                } else
                {
                    throw new IllegalArgumentException("Unknown type: " + typeName);
                }
            } else if (typeName.startsWith("vector") || typeName.startsWith("array"))
            {
                Matcher matcher = Pattern.compile("(vector|array)\\((\\d+)\\)").matcher(typeName);
                if (matcher.matches())
                {
                    int dimension = Integer.parseInt(matcher.group(1));
                    fieldType = TypeDescription.createVector(dimension);
                } else
                {
                    throw new IllegalArgumentException("Unknown type: " + typeName);
                }
            } else
            {
                switch (typeName)
                {
                    case "boolean":
                        fieldType = TypeDescription.createBoolean();
                        break;
                    case "tinyint":
                    case "byte":
                        fieldType = TypeDescription.createByte();
                        break;
                    case "smallint":
                    case "short":
                        fieldType = TypeDescription.createShort();
                        break;
                    case "integer":
                    case "int":
                        fieldType = TypeDescription.createInt();
                        break;
                    case "bigint":
                    case "long":
                        fieldType = TypeDescription.createLong();
                        break;
                    case "float":
                    case "real":
                        fieldType = TypeDescription.createFloat();
                        break;
                    case "double":
                        fieldType = TypeDescription.createDouble();
                        break;
                    case "string":
                        fieldType = TypeDescription.createString();
                        break;
                    case "date":
                        fieldType = TypeDescription.createDate();
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown type: " + typeName);
                }
            }
            schema.addField(typeName, fieldType);
        }
        return schema;
    }

    static class StringPosition
    {
        final String value;
        int position;
        final int length;

        StringPosition(String value)
        {
            this.value = value;
            position = 0;
            length = value.length();
        }

        @Override
        public String toString()
        {
            StringBuilder buffer = new StringBuilder();
            buffer.append('\'');
            buffer.append(value, 0, position);
            buffer.append('^');
            buffer.append(value.substring(position));
            buffer.append('\'');
            return buffer.toString();
        }
    }

    /**
     * Parse the category from the source.
     * For example, if the source is 'varchar(16)' with position = 0,
     * the returned category will be VARCHAR.
     * @param source the type name
     * @return
     */
    private static Category parseCategory(StringPosition source)
    {
        int start = source.position;
        while (source.position < source.length)
        {
            char ch = source.value.charAt(source.position);
            if (!Character.isLetter(ch))
            {
                break;
            }
            source.position += 1;
        }
        if (source.position != start)
        {
            String word = source.value.substring(start, source.position).toLowerCase();
            for (Category cat : Category.values())
            {
                if (cat.match(word))
                {
                    return cat;
                }
            }
        }
        throw new IllegalArgumentException("Can't parse type category at " + source);
    }

    /**
     * Parse an integer parameter from the source.
     * For example, if the source is 'varchar(16)' with position = 8,
     * the returned parameter will be 16.
     * @param source the type name
     * @return
     */
    private static int parseInt(StringPosition source)
    {
        int start = source.position;
        int result = 0;
        while (source.position < source.length)
        {
            char ch = source.value.charAt(source.position);
            if (!Character.isDigit(ch))
            {
                break;
            }
            result = result * 10 + (ch - '0');
            source.position += 1;
        }
        if (source.position == start)
        {
            throw new IllegalArgumentException("Missing integer at " + source);
        }
        return result;
    }


    public TypeDescription checkElementType(StringPosition source)
    {
        int start = source.position;
        String elementType;
        while (source.position < source.length) {
            char ch = source.value.charAt(source.position);
            if (ch==')') {
                elementType = source.value.substring(start, source.position);
                if (elementType.equalsIgnoreCase("double")) {
                    source.position++;
                    return this;
                } else {
                    throw new IllegalArgumentException("For Pixels vector type, the trino type should be array(double), but instead is " + source.value);
                }
            }
            source.position++;
        }
        throw new IllegalArgumentException("For Pixels vector type, the trino type should be array(double), but instead is " + source.value);
    }

    /**
     * Parse the field name, i.e., user-defined identifier, from the source.
     * For example, if the source is 'a:int' with position = 0,
     * the returned name will be 'a'.
     * @param source
     * @return
     */
    private static String parseName(StringPosition source)
    {
        if (source.position == source.length)
        {
            throw new IllegalArgumentException("Missing name at " + source);
        }
        final int start = source.position;
        if (source.value.charAt(source.position) == '`')
        {
            source.position += 1;
            StringBuilder buffer = new StringBuilder();
            boolean closed = false;
            while (source.position < source.length)
            {
                char ch = source.value.charAt(source.position);
                source.position += 1;
                if (ch == '`')
                {
                    if (source.position < source.length &&
                            source.value.charAt(source.position) == '`')
                    {
                        source.position += 1;
                        buffer.append('`');
                    }
                    else
                    {
                        closed = true;
                        break;
                    }
                }
                else
                {
                    buffer.append(ch);
                }
            }
            if (!closed)
            {
                source.position = start;
                throw new IllegalArgumentException("Unmatched quote at " + source);
            }
            else if (buffer.length() == 0)
            {
                throw new IllegalArgumentException("Empty quoted field name at " + source);
            }
            return buffer.toString();
        }
        else
        {
            while (source.position < source.length)
            {
                char ch = source.value.charAt(source.position);
                if (!Character.isLetterOrDigit(ch) && ch != '.' && ch != '_')
                {
                    break;
                }
                source.position += 1;
            }
            if (source.position == start)
            {
                throw new IllegalArgumentException("Missing name at " + source);
            }
            return source.value.substring(start, source.position);
        }
    }

    private static void requireChar(StringPosition source, char required)
    {
        if (source.position >= source.length ||
                source.value.charAt(source.position) != required)
        {
            throw new IllegalArgumentException("Missing required char '" +
                    required + "' at " + source);
        }
        source.position += 1;
    }

    private static boolean consumeChar(StringPosition source, char ch)
    {
        boolean result = source.position < source.length &&
                source.value.charAt(source.position) == ch;
        if (result)
        {
            source.position += 1;
        }
        return result;
    }

    private static void parseStruct(TypeDescription type, StringPosition source)
    {
        requireChar(source, '<');
        do
        {
            String fieldName = parseName(source);
            requireChar(source, ':');
            type.addField(fieldName, parseType(source));
        } while (consumeChar(source, ','));
        requireChar(source, '>');
    }

    private static TypeDescription parseType(StringPosition source)
    {
        TypeDescription result = new TypeDescription(parseCategory(source));
        switch (result.getCategory())
        {
            case BOOLEAN:
            case BYTE:
            case DATE:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
            case STRING:
                break;
            case TIME:
                if (consumeChar(source, '('))
                {
                    // with precision specified
                    result.withPrecision(parseInt(source));
                    requireChar(source, ')');
                }
                else
                {
                    result.withPrecision(DEFAULT_TIME_PRECISION);
                }
            case TIMESTAMP:
                if (consumeChar(source, '('))
                {
                    // with precision specified
                    result.withPrecision(parseInt(source));
                    requireChar(source, ')');
                }
                else
                {
                    result.withPrecision(DEFAULT_TIMESTAMP_PRECISION);
                }
                break;
            case BINARY:
            case VARBINARY:
            case CHAR:
            case VARCHAR:
                if (consumeChar(source, '('))
                {
                    // with length specified
                    result.withMaxLength(parseInt(source));
                    requireChar(source, ')');
                }
                else if (result.getCategory() == Category.CHAR)
                {
                    // default char length of 1 is from SQL standard
                    result.withMaxLength(DEFAULT_CHAR_LENGTH);
                }
                else
                {
                    result.withMaxLength(DEFAULT_VARCHAR_OR_BINARY_LENGTH);
                }
                break;
            case DECIMAL:
                if (consumeChar(source, '('))
                {
                    int precision = parseInt(source);
                    // default scale of 0 is from SQL standard
                    int scale = 0;
                    if (consumeChar(source, ','))
                    {
                        scale = parseInt(source);
                    }
                    requireChar(source, ')');
                    if (scale > precision)
                    {
                        throw new IllegalArgumentException("Decimal's scale " + scale +
                                " is larger than its precision " + precision);
                    }
                    result.withPrecision(precision);
                    result.withScale(scale);
                }
                else
                {
                    // precision is 38 by default, while scale is 0 by default
                    result.withPrecision(DEFAULT_LONG_DECIMAL_PRECISION);
                    result.withScale(DEFAULT_DECIMAL_SCALE);
                }
                break;
            case STRUCT:
                parseStruct(result, source);
                break;
            case VECTOR:
                {
                    // handle type string passed from trino. Check that the type is double(array).
                    if (source.value.contains("array")) {
                        if (consumeChar(source, '('))
                        {
                            // the array type must be double
                            result.checkElementType(source);
                            result.withDimension(DEFAULT_VECTOR_DIMENSION);
                        }
                    }
                    // handle type string passed from Pixels writer, should be vector(d), where (d) specifies that
                    // this column has dimension d, and is optional
                    else if (source.value.contains("vector")) {
                        if (consumeChar(source, '(')) {
                            // with dimension specified
                            result.withDimension(parseInt(source));
                            requireChar(source, ')');
                        } else {
                            result.withDimension(DEFAULT_VECTOR_DIMENSION);
                        }
                    }  else {
                        throw new IllegalArgumentException("Unknown type string" +
                                result + " at " + source);
                    }
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown type " +
                        result.getCategory() + " at " + source);
        }
        return result;
    }

    /**
     * Parse TypeDescription from the type name. This is the inverse
     * of TypeDescription.toString().
     * An example of the type name is:
     * <pre>{@code struct<a:int,b:decimal(15,2),c:timestamp,d:varchar(16)>}</pre>
     *
     * @param typeName the name of the type
     * @return a new TypeDescription or null if typeName was null
     * @throws IllegalArgumentException if the string is badly formed
     */
    public static TypeDescription fromString(String typeName)
    {
        if (typeName == null)
        {
            return null;
        }
        StringPosition source = new StringPosition( // remove whitespaces (if any)
                typeName.replaceAll("\\s", ""));
        TypeDescription result = parseType(source);
        if (source.position != source.length)
        {
            throw new IllegalArgumentException("Extra characters at " + source);
        }
        return result;
    }

    /**
     * Check if the typeName can be parsed to a valid type description.
     * @param typeName
     * @return
     */
    public static boolean isValid(String typeName)
    {
        try
        {
            TypeDescription type = fromString(typeName);
            return type != null;
        } catch (IllegalArgumentException e)
        {
            return false;
        }
    }

    /**
     * Write the schema into the Pixels footer builder.
     * @param builder
     * @param schema
     */
    public static void writeTypes(PixelsProto.Footer.Builder builder, TypeDescription schema)
    {
        List<TypeDescription> children = schema.getChildren();
        List<String> names = schema.getFieldNames();
        if (children == null || children.isEmpty())
        {
            return;
        }
        for (int i = 0; i < children.size(); i++)
        {
            TypeDescription child = children.get(i);
            PixelsProto.Type.Builder tmpType = PixelsProto.Type.newBuilder();
            tmpType.setName(names.get(i));
            switch (child.getCategory())
            {
                case BOOLEAN:
                    tmpType.setKind(PixelsProto.Type.Kind.BOOLEAN);
                    break;
                case BYTE:
                    tmpType.setKind(PixelsProto.Type.Kind.BYTE);
                    break;
                case SHORT:
                    tmpType.setKind(PixelsProto.Type.Kind.SHORT);
                    break;
                case INT:
                    tmpType.setKind(PixelsProto.Type.Kind.INT);
                    break;
                case LONG:
                    tmpType.setKind(PixelsProto.Type.Kind.LONG);
                    break;
                case FLOAT:
                    tmpType.setKind(PixelsProto.Type.Kind.FLOAT);
                    break;
                case DOUBLE:
                    tmpType.setKind(PixelsProto.Type.Kind.DOUBLE);
                    break;
                case DECIMAL:
                    tmpType.setKind(PixelsProto.Type.Kind.DECIMAL);
                    tmpType.setPrecision(child.precision);
                    tmpType.setScale(child.scale);
                    break;
                case STRING:
                    tmpType.setKind(PixelsProto.Type.Kind.STRING);
                    break;
                case CHAR:
                    tmpType.setKind(PixelsProto.Type.Kind.CHAR);
                    tmpType.setMaximumLength(child.getMaxLength());
                    break;
                case VARCHAR:
                    tmpType.setKind(PixelsProto.Type.Kind.VARCHAR);
                    tmpType.setMaximumLength(child.getMaxLength());
                    break;
                case BINARY:
                    tmpType.setKind(PixelsProto.Type.Kind.BINARY);
                    tmpType.setMaximumLength(child.getMaxLength());
                    break;
                case VARBINARY:
                    tmpType.setKind(PixelsProto.Type.Kind.VARBINARY);
                    tmpType.setMaximumLength(child.getMaxLength());
                    break;
                case TIMESTAMP:
                    tmpType.setKind(PixelsProto.Type.Kind.TIMESTAMP);
                    tmpType.setPrecision(child.getPrecision());
                    break;
                case DATE:
                    tmpType.setKind(PixelsProto.Type.Kind.DATE);
                    break;
                case TIME:
                    tmpType.setKind(PixelsProto.Type.Kind.TIME);
                    tmpType.setPrecision(child.getPrecision());
                    break;
                case VECTOR:
                    tmpType.setKind(PixelsProto.Type.Kind.VECTOR);
                    tmpType.setDimension(child.getDimension());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown category: " +
                            schema.getCategory());
            }
            builder.addTypes(tmpType.build());
        }
    }

    /**
     * For decimal types, set the precision.
     *
     * @param precision the new precision
     * @return this
     */
    public TypeDescription withPrecision(int precision)
    {
        if (category == Category.DECIMAL)
        {
            if (precision < 1 || precision > MAX_LONG_DECIMAL_PRECISION)
            {
                throw new IllegalArgumentException("precision " + precision +
                        " is out of the valid range 1 .. " + MAX_LONG_DECIMAL_PRECISION);
            } else if (scale > precision)
            {
                throw new IllegalArgumentException("precision " + precision +
                        " is smaller that scale " + scale);
            }
        }
        else if (category == Category.TIMESTAMP)
        {
            if (precision < 0 || precision > MAX_TIMESTAMP_PRECISION)
            {
                throw new IllegalArgumentException("precision " + precision +
                        " is out of the valid range 0 .. " + MAX_TIMESTAMP_PRECISION);
            }
        }
        else if (category == Category.TIME)
        {
            if (precision < 0 || precision > MAX_TIME_PRECISION)
            {
                throw new IllegalArgumentException("precision " + precision +
                        " is out of the valid range 0 .. " + MAX_TIME_PRECISION);
            }
        }
        else
        {
            throw new IllegalArgumentException("precision is only valid on decimal" +
                    ", time, and timestamp, but not " + category.primaryName);
        }
        this.precision = precision;
        return this;
    }

    /**
     * For decimal types, set the scale.
     *
     * @param scale the new scale
     * @return this
     */
    public TypeDescription withScale(int scale)
    {
        if (category == Category.DECIMAL)
        {
            if (scale < 0 || scale > MAX_LONG_DECIMAL_SCALE)
            {
                throw new IllegalArgumentException("scale " + scale +
                        " is out of the valid range 0 .. " + MAX_LONG_DECIMAL_SCALE);
            } else if (scale > precision)
            {
                throw new IllegalArgumentException("scale " + scale +
                        " is out of the valid range 0 .. " + precision);
            }
        }
        else
        {
            throw new IllegalArgumentException("scale is only valid on decimal" +
                    ", but not " + category.primaryName);
        }
        this.scale = scale;
        return this;
    }

    /**
     * Set the maximum length for char/binary and varchar/varbinary types.
     *
     * @param maxLength the maximum value
     * @return this
     */
    public TypeDescription withMaxLength(int maxLength)
    {
        if (category != Category.VARCHAR && category != Category.CHAR &&
        category != Category.BINARY && category != Category.VARBINARY)
        {
            throw new IllegalArgumentException("maxLength is only allowed on char" +
                    ", varchar, binary, and varbinary, but not " + category.primaryName);
        }
        if (maxLength < 1)
        {
            throw new IllegalArgumentException("maxLength " + maxLength + " is not positive");
        }
        this.maxLength = maxLength;
        return this;
    }

    /**
     * Set the dimension for the column of vector type
     *
     * @param dimension the dimension of the vector column
     * @return this
     */
    public TypeDescription withDimension(int dimension)
    {
        if (category != Category.VECTOR)
        {
            throw new IllegalArgumentException("dimension is only allowed on vector type but not on " + category.primaryName);
        }
        if (dimension < 1)
        {
            throw new IllegalArgumentException("dimension " + dimension + " is not positive");
        }
        if (dimension > MAXIMUM_VECTOR_DIMENSION) {
            throw new IllegalArgumentException("dimension" + dimension + "currently supported maximum dimension " + MAXIMUM_VECTOR_DIMENSION);
        }
        this.dimension = dimension;
        return this;
    }

    /**
     * Add a field to a struct type as it is built.
     *
     * @param field     the field name
     * @param fieldType the type of the field
     * @return the struct type
     */
    public TypeDescription addField(String field, TypeDescription fieldType)
    {
        if (category != Category.STRUCT)
        {
            throw new IllegalArgumentException("Can only add fields to struct type" +
                    " but not " + category);
        }
        fieldNames.add(requireNonNull(field, "filed is null"));
        children.add(requireNonNull(fieldType, "filedType is null"));
        fieldType.parent = this;
        return this;
    }

    /**
     * Get the id for this type.
     * The first call will cause all the ids in tree to be assigned, so
     * it should not be called before the type is completely built.
     *
     * @return the sequential id
     */
    public int getId()
    {
        // if the id hasn't been assigned, assign all the ids from the root
        if (id == -1)
        {
            TypeDescription root = this;
            while (root.parent != null)
            {
                root = root.parent;
            }
            root.assignIds(0);
        }
        return id;
    }

    public TypeDescription clone()
    {
        TypeDescription result = new TypeDescription(category);
        result.maxLength = maxLength;
        result.precision = precision;
        result.scale = scale;
        if (fieldNames != null)
        {
            result.fieldNames.addAll(fieldNames);
        }
        if (children != null)
        {
            for (TypeDescription child : children)
            {
                TypeDescription clone = requireNonNull(child.clone(), "failed to clone child");
                clone.parent = result;
                result.children.add(clone);
            }
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        long result = category.ordinal() * 4241L + maxLength + precision * 13L + scale;
        if (children != null)
        {
            for (TypeDescription child : children)
            {
                result = result * 6959 + child.hashCode();
            }
        }
        return (int) result;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof TypeDescription))
        {
            return false;
        }
        if (other == this)
        {
            return true;
        }
        TypeDescription castOther = (TypeDescription) other;
        if (category != castOther.category ||
                maxLength != castOther.maxLength ||
                scale != castOther.scale ||
                precision != castOther.precision)
        {
            return false;
        }
        if (children != null)
        {
            if (children.size() != castOther.children.size())
            {
                return false;
            }
            for (int i = 0; i < children.size(); ++i)
            {
                if (!children.get(i).equals(castOther.children.get(i)))
                {
                    return false;
                }
            }
        }
        if (category == Category.STRUCT)
        {
            for (int i = 0; i < fieldNames.size(); ++i)
            {
                if (!fieldNames.get(i).equals(castOther.fieldNames.get(i)))
                {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Get the maximum id assigned to this type or its children.
     * The first call will cause all the ids in tree to be assigned, so
     * it should not be called before the type is completely built.
     *
     * @return the maximum id assigned under this type
     */
    public int getMaximumId()
    {
        // if the id hasn't been assigned, assign all the ids from the root
        if (maxId == -1)
        {
            TypeDescription root = this;
            while (root.parent != null)
            {
                root = root.parent;
            }
            root.assignIds(0);
        }
        return maxId;
    }

    private ColumnVector createColumn(int maxSize, int mode, boolean... useEncodedVector)
    {
        requireNonNull(useEncodedVector, "columnsEncoded should not be null");
        // the length of useEncodedVector is already checked, not need to check again.
        switch (category)
        {
            case BOOLEAN:
            case BYTE:
                return new ByteColumnVector(maxSize);
            case SHORT:
            case INT:
                if (Mode.match(mode, Mode.CREATE_INT_VECTOR_FOR_INT))
                {
                    return new IntColumnVector(maxSize);
                }
                else
                {
                    return new LongColumnVector(maxSize);
                }
            case LONG:
                return new LongColumnVector(maxSize);
            case DATE:
                return new DateColumnVector(maxSize);
            case TIME:
                return new TimeColumnVector(maxSize, precision);
            case TIMESTAMP:
                return new TimestampColumnVector(maxSize, precision);
            case FLOAT:
                return new FloatColumnVector(maxSize);
            case DOUBLE:
                return new DoubleColumnVector(maxSize);
            case DECIMAL:
                if (precision <= MAX_SHORT_DECIMAL_PRECISION)
                    return new DecimalColumnVector(maxSize, precision, scale);
                else
                    return new LongDecimalColumnVector(maxSize, precision, scale);
            case STRING:
            case BINARY:
            case VARBINARY:
            case CHAR:
            case VARCHAR:
                if (!useEncodedVector[0])
                {
                    return new BinaryColumnVector(maxSize);
                }
                else
                {
                    return new DictionaryColumnVector(maxSize);
                }
            case STRUCT:
            {
                ColumnVector[] fieldVector = new ColumnVector[children.size()];
                for (int i = 0; i < fieldVector.length; ++i)
                {
                    fieldVector[i] = children.get(i).createColumn(maxSize, mode, useEncodedVector[i]);
                }
                return new StructColumnVector(maxSize, fieldVector);
            }
            case VECTOR:
                return new VectorColumnVector(maxSize, dimension);
            default:
                throw new IllegalArgumentException("Unknown type " + category);
        }
    }

    public VectorizedRowBatch createRowBatch(int maxSize, int mode, boolean... useEncodedVector)
    {
        VectorizedRowBatch result;
        if (category == Category.STRUCT)
        {
            checkArgument(useEncodedVector.length == 0 || useEncodedVector.length == children.size(),
                    "there must be 0 or children.size() elements in useEncodedVector");
            result = new VectorizedRowBatch(children.size(), maxSize);
            List<String> columnNames = new ArrayList<>();
            for (int i = 0; i < result.cols.length; ++i)
            {
                String fieldName = fieldNames.get(i);
                ColumnVector cv = children.get(i).createColumn(maxSize, mode,
                        useEncodedVector.length != 0 && useEncodedVector[i]);
                int originId = columnNames.indexOf(fieldName);
                if (originId >= 0)
                {
                    cv.duplicated = true;
                    cv.originVecId = originId;
                }
                else
                {
                    columnNames.add(fieldName);
                }
                result.cols[i] = cv;
            }
        }
        else
        {
            checkArgument(useEncodedVector.length == 0 || useEncodedVector.length == 1,
                    "for null structure type, there can be only 0 or 1 elements in useEncodedVector");
            result = new VectorizedRowBatch(1, maxSize);
            result.cols[0] = createColumn(maxSize, mode,
                    useEncodedVector.length == 1 && useEncodedVector[0]);
        }
        result.reset();
        return result;
    }

    public VectorizedRowBatch createRowBatch()
    {
        return createRowBatch(VectorizedRowBatch.DEFAULT_SIZE, Mode.NONE);
    }

    public VectorizedRowBatch createRowBatch(int size)
    {
        return createRowBatch(size, Mode.NONE);
    }

    public VectorizedRowBatch createRowBatchWithHiddenColumn(int maxSize, int mode, boolean... useEncodedVector)
    {
        VectorizedRowBatch result;
        if (category == Category.STRUCT)
        {
            // hidden column is long type, so no need to encode
            checkArgument(useEncodedVector.length == 0 || useEncodedVector.length == children.size(),
                    "there must be 0 or children.size() elements in useEncodedVector");
            result = new VectorizedRowBatch(children.size() + 1, maxSize);
            List<String> columnNames = new ArrayList<>();
            for (int i = 0; i < result.cols.length - 1; ++i)
            {
                String fieldName = fieldNames.get(i);
                ColumnVector cv = children.get(i).createColumn(maxSize, mode,
                        useEncodedVector.length != 0 && useEncodedVector[i]);
                int originId = columnNames.indexOf(fieldName);
                if (originId >= 0)
                {
                    cv.duplicated = true;
                    cv.originVecId = originId;
                }
                else
                {
                    columnNames.add(fieldName);
                }
                result.cols[i] = cv;
            }
            // add hidden column
            result.cols[result.cols.length - 1] = new LongColumnVector(maxSize);
        }
        else
        {
            checkArgument(useEncodedVector.length == 0 || useEncodedVector.length == 1,
                    "for null structure type, there can be only 0 or 1 elements in useEncodedVector");
            result = new VectorizedRowBatch(2, maxSize);
            result.cols[0] = createColumn(maxSize, mode,
                    useEncodedVector.length == 1 && useEncodedVector[0]);
            result.cols[1] = new LongColumnVector(maxSize);
        }
        result.reset();
        return result;
    }

    public VectorizedRowBatch createRowBatchWithHiddenColumn()
    {
        return createRowBatchWithHiddenColumn(VectorizedRowBatch.DEFAULT_SIZE, Mode.NONE);
    }

    /**
     * Get the kind of this type.
     *
     * @return get the category for this type.
     */
    public Category getCategory()
    {
        return category;
    }

    /**
     * Get the maximum length of the type. Only used for char and varchar types.
     *
     * @return the maximum length of the string type
     */
    public int getMaxLength()
    {
        return maxLength;
    }

    /**
     * Get the dimension of the vector type
     *
     * @return the dimension of the vector type
     */
    public int getDimension()
    {
        return dimension;
    }

    /**
     * Get the precision of the decimal type.
     *
     * @return the number of digits for the precision.
     */
    public int getPrecision()
    {
        return precision;
    }

    /**
     * Get the scale of the decimal type.
     *
     * @return the number of digits for the scale.
     */
    public int getScale()
    {
        return scale;
    }

    /**
     * For struct types, get the list of field names.
     *
     * @return the list of field names.
     */
    public List<String> getFieldNames()
    {
        return Collections.unmodifiableList(requireNonNull(fieldNames, "filedNames is null"));
    }

    /**
     * Get the subtypes of this type.
     *
     * @return the list of children types
     */
    public List<TypeDescription> getChildren()
    {
        return children == null ? null : Collections.unmodifiableList(children);
    }

    public List<TypeDescription> getChildrenWithHiddenColumn()
    {
        List<TypeDescription> result = new ArrayList<>(children);
        result.add(new TypeDescription(Category.LONG));
        return Collections.unmodifiableList(result);
    }

    /**
     * Assign ids to all the nodes under this one.
     *
     * @param startId the lowest id to assign
     * @return the next available id
     */
    private int assignIds(int startId)
    {
        id = startId++;
        if (children != null)
        {
            for (TypeDescription child : children)
            {
                startId = child.assignIds(startId);
            }
        }
        maxId = startId - 1;
        return startId;
    }

    public TypeDescription(Category category)
    {
        this.category = category;
        if (category.isPrimitive)
        {
            children = null;
        }
        else
        {
            children = new ArrayList<>();
        }
        if (category == Category.STRUCT)
        {
            fieldNames = new ArrayList<>();
        }
        else
        {
            fieldNames = null;
        }
    }

    private int id = -1;
    private int maxId = -1;
    private TypeDescription parent;
    private final Category category;
    private final List<TypeDescription> children;
    private final List<String> fieldNames;
    private int maxLength = DEFAULT_VARCHAR_OR_BINARY_LENGTH;
    private int precision = DEFAULT_SHORT_DECIMAL_PRECISION;
    private int scale = DEFAULT_DECIMAL_SCALE;
    private int dimension = DEFAULT_VECTOR_DIMENSION;

    static void printFieldName(StringBuilder buffer, String name)
    {
        if (UNQUOTED_NAMES.matcher(name).matches())
        {
            buffer.append(name);
        }
        else
        {
            buffer.append('`');
            buffer.append(name.replace("`", "``"));
            buffer.append('`');
        }
    }

    public void printToBuffer(StringBuilder buffer)
    {
        buffer.append(category.primaryName);
        switch (category)
        {
            case BINARY:
            case VARBINARY:
            case CHAR:
            case VARCHAR:
                buffer.append('(');
                buffer.append(maxLength);
                buffer.append(')');
                break;
            case DECIMAL:
                buffer.append('(');
                buffer.append(precision);
                buffer.append(',');
                buffer.append(scale);
                buffer.append(')');
                break;
            case TIME:
            case TIMESTAMP:
                buffer.append('(');
                buffer.append(precision);
                buffer.append(')');
                break;
            case STRUCT:
                buffer.append('<');
                for (int i = 0; i < children.size(); ++i)
                {
                    if (i != 0)
                    {
                        buffer.append(',');
                    }
                    printFieldName(buffer, fieldNames.get(i));
                    buffer.append(':');
                    children.get(i).printToBuffer(buffer);
                }
                buffer.append('>');
                break;
            default:
                break;
        }
    }

    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        printToBuffer(buffer);
        return buffer.toString();
    }

    private void printJsonToBuffer(String prefix, StringBuilder buffer,
                                   int indent)
    {
        for (int i = 0; i < indent; ++i)
        {
            buffer.append(' ');
        }
        buffer.append(prefix);
        buffer.append("{\"category\": \"");
        buffer.append(category.primaryName);
        buffer.append("\", \"id\": ");
        buffer.append(getId());
        buffer.append(", \"maxId\": ");
        buffer.append(maxId);
        switch (category)
        {
            case BINARY:
            case VARBINARY:
            case CHAR:
            case VARCHAR:
                buffer.append(", \"maxLength\": ");
                buffer.append(maxLength);
                break;
            case DECIMAL:
                buffer.append(", \"precision\": ");
                buffer.append(precision);
                buffer.append(", \"scale\": ");
                buffer.append(scale);
                break;
            case TIME:
            case TIMESTAMP:
                buffer.append(", \"precision\": ");
                buffer.append(precision);
                break;
            case STRUCT:
                buffer.append(", \"fields\": [");
                for (int i = 0; i < children.size(); ++i)
                {
                    buffer.append('\n');
                    children.get(i).printJsonToBuffer("\"" + fieldNames.get(i) + "\": ",
                            buffer, indent + 2);
                    if (i != children.size() - 1)
                    {
                        buffer.append(',');
                    }
                }
                buffer.append(']');
                break;
            default:
                break;
        }
        buffer.append('}');
    }

    public String toJson()
    {
        StringBuilder buffer = new StringBuilder();
        printJsonToBuffer("", buffer, 0);
        return buffer.toString();
    }

    /**
     * Locate a subtype by its id.
     *
     * @param goal the column id to look for
     * @return the subtype
     */
    public TypeDescription findSubtype(int goal)
    {
        // call getId method to make sure the ids are assigned
        int id = getId();
        if (goal < id || goal > maxId)
        {
            throw new IllegalArgumentException("Unknown type id " + id + " in " +
                    toJson());
        }
        if (goal == id)
        {
            return this;
        }
        else
        {
            TypeDescription prev = null;
            for (TypeDescription next : children)
            {
                if (next.id > goal)
                {
                    return prev.findSubtype(goal);
                }
                prev = next;
            }
            return prev.findSubtype(goal);
        }
    }

    /**
     * The type related modes used when creating column vectors and row batches.
     */
    public static final class Mode
    {
        public static final int NONE = 0;
        /**
         * Create {@link IntColumnVector} for INT type.
         */
        public static final int CREATE_INT_VECTOR_FOR_INT = 0x01;

        public static boolean match(int mode1, int mode2)
        {
            return (mode1 & mode2) != 0;
        }
    }
}
