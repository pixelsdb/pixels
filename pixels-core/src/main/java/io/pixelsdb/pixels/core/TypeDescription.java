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
import io.pixelsdb.pixels.core.vector.*;

import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * TypeDescription derived from org.apache.orc
 * <p>
 * Schema description in a Pixels file.
 */
public final class TypeDescription
        implements Comparable<TypeDescription>, Serializable, Cloneable
{
    private static final long serialVersionUID = 4270695889340023552L;
    /**
     * Issue #196:
     * In SQL standard, the max precision of decimal is 38.
     * However, we only support short decimal of which the max precision is 18.
     */
    public static final int MAX_PRECISION = 18;
    /**
     * Issue #196:
     * In SQL standard, the max scale of decimal is 38.
     * However, we only support short decimal of which the max scale is 18.
     */
    public static final int MAX_SCALE = 18;
    /**
     * Issue #196:
     * In SQL standard, the default precision of decimal is 38.
     * However, we only support short decimal of which the default precision is 18.
     */
    public static final int DEFAULT_PRECISION = 18;
    /**
     * It is a standard that the default scale of decimal is 0.
     */
    public static final int DEFAULT_SCALE = 0;
    /**
     * The default length of varchar, binary, and varbinary.
     */
    public static final int DEFAULT_LENGTH = 65535;
    /**
     * It is a standard that the default length of char is 1.
     */
    public static final int DEFAULT_CHAR_LENGTH = 1;
    private static final Pattern UNQUOTED_NAMES = Pattern.compile("^\\w+$");

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
                    case DECIMAL:
                        result = precision - other.precision;
                        if (result == 0)
                        {
                            result = scale = other.scale;
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
        DECIMAL(true, double.class, long.class, "decimal"),
        STRING(true, String.class, byte[].class, "string"),
        DATE(true, Date.class, int.class, "date"),
        TIME(true, Time.class, int.class, "time"),
        TIMESTAMP(true, Timestamp.class, long.class, "timestamp"),
        VARBINARY(true, byte[].class, byte[].class, "varbinary"),
        BINARY(true, byte[].class, byte[].class, "binary"),
        VARCHAR(true, byte[].class, byte[].class,"varchar"),
        CHAR(true, byte[].class, byte[].class,"char"),
        STRUCT(false, Class.class, Class.class, "struct");

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

    public static TypeDescription createDecimal()
    {
        return new TypeDescription(Category.DECIMAL);
    }

    public static TypeDescription createString()
    {
        return new TypeDescription(Category.STRING);
    }

    public static TypeDescription createDate()
    {
        return new TypeDescription(Category.DATE);
    }

    public static TypeDescription createTime()
    {
        return new TypeDescription(Category.TIME);
    }

    public static TypeDescription createTimestamp()
    {
        return new TypeDescription(Category.TIMESTAMP);
    }

    public static TypeDescription createVarbinary()
    {
        return new TypeDescription(Category.VARBINARY);
    }

    public static TypeDescription createBinary()
    {
        return new TypeDescription(Category.BINARY);
    }

    public static TypeDescription createVarchar()
    {
        return new TypeDescription(Category.VARCHAR);
    }

    public static TypeDescription createChar()
    {
        return new TypeDescription(Category.CHAR);
    }

    public static TypeDescription createStruct()
    {
        return new TypeDescription(Category.STRUCT);
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
                    fieldType = TypeDescription.createDecimal();
                    fieldType.precision = type.getPrecision();
                    fieldType.scale = type.getScale();
                    break;
                case VARCHAR:
                    fieldType = TypeDescription.createVarchar();
                    fieldType.maxLength = type.getMaximumLength();
                    break;
                case CHAR:
                    fieldType = TypeDescription.createChar();
                    fieldType.maxLength = type.getMaximumLength();
                    break;
                case STRING:
                    fieldType = TypeDescription.createString();
                    break;
                case DATE:
                    fieldType = TypeDescription.createDate();
                    break;
                case TIME:
                    fieldType = TypeDescription.createTime();
                    break;
                case TIMESTAMP:
                    fieldType = TypeDescription.createTimestamp();
                    break;
                case VARBINARY:
                    fieldType = TypeDescription.createVarbinary();
                    fieldType.maxLength = type.getMaximumLength();
                    break;
                case BINARY:
                    fieldType = TypeDescription.createBinary();
                    fieldType.maxLength = type.getMaximumLength();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type: " +
                            type.getKind());
            }
            schema.addField(fieldName, fieldType);
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
            buffer.append(value.substring(0, position));
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
            case TIME:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
            case STRING:
            case TIMESTAMP:
                break;
            case BINARY:
            case VARBINARY:
            case CHAR:
            case VARCHAR:
                if (consumeChar(source, '('))
                {
                    // with length specified.
                    result.withMaxLength(parseInt(source));
                    requireChar(source, ')');
                }
                else if (result.getCategory() == Category.CHAR)
                {
                    // It is a standard that the default length of char is 1.
                    result.withMaxLength(DEFAULT_CHAR_LENGTH);
                }
                break;
            case DECIMAL:
                if (consumeChar(source, '('))
                {
                    int precision = parseInt(source);
                    // It is a standard that scale is 0 by default.
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
                } // precision is 38 by default, while scale is 0 by default.
                break;
            case STRUCT:
                parseStruct(result, source);
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
                    break;
                case DATE:
                    tmpType.setKind(PixelsProto.Type.Kind.DATE);
                    break;
                case TIME:
                    tmpType.setKind(PixelsProto.Type.Kind.TIME);
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
        if (precision < 1)
        {
            throw new IllegalArgumentException("precision " + precision + " is negative");
        }
        else if (precision > MAX_PRECISION)
        {
            throw new IllegalArgumentException("precision " + precision +
                    " is out of the max precision " + MAX_PRECISION);
        }
        else if (scale > precision)
        {
            throw new IllegalArgumentException("precision " + precision +
                    " is smaller that scale " + scale);
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
        if (scale < 0)
        {
            throw new IllegalArgumentException("scale " + scale + " is negative");
        }
        else if (scale > MAX_SCALE)
        {
            throw new IllegalArgumentException("scale " + scale +
                    " is out of the max scale " + MAX_SCALE);
        }
        else if (scale > precision)
        {
            throw new IllegalArgumentException("scale " + scale +
                    " is out of range 0 .. " + precision);
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
        this.maxLength = maxLength;
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
        fieldNames.add(field);
        children.add(fieldType);
        fieldType.parent = this;
        return this;
    }

    /**
     * Get the id for this type.
     * The first call will cause all of the the ids in tree to be assigned, so
     * it should not be called before the type is completely built.
     *
     * @return the sequential id
     */
    public int getId()
    {
        // if the id hasn't been assigned, assign all of the ids from the root
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
                TypeDescription clone = child.clone();
                clone.parent = result;
                result.children.add(clone);
            }
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        long result = category.ordinal() * 4241 + maxLength + precision * 13 + scale;
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
        if (other == null || !(other instanceof TypeDescription))
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
     * The first call will cause all of the the ids in tree to be assigned, so
     * it should not be called before the type is completely built.
     *
     * @return the maximum id assigned under this type
     */
    public int getMaximumId()
    {
        // if the id hasn't been assigned, assign all of the ids from the root
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

    private ColumnVector createColumn(int maxSize)
    {
        switch (category)
        {
            case BOOLEAN:
            case BYTE:
                return new ByteColumnVector(maxSize);
            case SHORT:
            case INT:
            case LONG:
                return new LongColumnVector(maxSize);
            case DATE:
                return new DateColumnVector(maxSize);
            case TIME:
                return new TimeColumnVector(maxSize);
            case TIMESTAMP:
                return new TimestampColumnVector(maxSize);
            case FLOAT:
            case DOUBLE:
                return new DoubleColumnVector(maxSize);
            case DECIMAL:
                return new DecimalColumnVector(maxSize, precision, scale);
            case STRING:
            case BINARY:
            case VARBINARY:
            case CHAR:
            case VARCHAR:
                return new BinaryColumnVector(maxSize);
            case STRUCT:
            {
                ColumnVector[] fieldVector = new ColumnVector[children.size()];
                for (int i = 0; i < fieldVector.length; ++i)
                {
                    fieldVector[i] = children.get(i).createColumn(maxSize);
                }
                return new StructColumnVector(maxSize,
                        fieldVector);
            }
            default:
                throw new IllegalArgumentException("Unknown type " + category);
        }
    }

    public VectorizedRowBatch createRowBatch(int maxSize)
    {
        VectorizedRowBatch result;
        if (category == Category.STRUCT)
        {
            result = new VectorizedRowBatch(children.size(), maxSize);
            List<String> columnNames = new ArrayList<>();
            for (int i = 0; i < result.cols.length; ++i)
            {
                String fieldName = fieldNames.get(i);
                ColumnVector cv = children.get(i).createColumn(maxSize);
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
            result = new VectorizedRowBatch(1, maxSize);
            result.cols[0] = createColumn(maxSize);
        }
        result.reset();
        return result;
    }

    public VectorizedRowBatch createRowBatch()
    {
        return createRowBatch(VectorizedRowBatch.DEFAULT_SIZE);
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
        return Collections.unmodifiableList(fieldNames);
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

    /**
     * Assign ids to all of the nodes under this one.
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
    private int maxLength = DEFAULT_LENGTH;
    private int precision = DEFAULT_PRECISION;
    private int scale = DEFAULT_SCALE;

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
}
