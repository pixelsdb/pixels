/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.pixels.hive;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.hive
 * @ClassName: PixelsStruct
 * @Description: refer: [PixelsStruct](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/orc/PixelsStruct.java)
 * @author: tao
 * @date: Create in 2018-12-12 22:35
 **/
final public class PixelsStruct implements Writable {
    private static Logger log = LogManager.getLogger(PixelsStruct.class);

    private Object[] fields;

    public PixelsStruct(int children) {
        fields = new Object[children];
    }

    public Object getFieldValue(int fieldIndex) {
        return fields[fieldIndex];
    }

    public void setFieldValue(int fieldIndex, Object value) {
        fields[fieldIndex] = value;
    }

    public int getNumFields() {
        return fields.length;
    }

    /**
     * Change the number of fields in the struct. No effect if the number of
     * fields is the same. The old field values are copied to the new array.
     *
     * @param numFields the new number of fields
     */
    public void setNumFields(int numFields) {
        if (fields.length != numFields) {
            Object[] oldFields = fields;
            fields = new Object[numFields];
            System.arraycopy(oldFields, 0, fields, 0,
                    Math.min(oldFields.length, numFields));
        }
    }

    /**
     * Destructively make this object link to other's values.
     *
     * @param other the value to point to
     */
    void linkFields(PixelsStruct other) {
        fields = other.fields;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        throw new UnsupportedOperationException("write unsupported");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        throw new UnsupportedOperationException("readFields unsupported");
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || other.getClass() != PixelsStruct.class) {
            return false;
        } else {
            PixelsStruct oth = (PixelsStruct) other;
            if (fields.length != oth.fields.length) {
                return false;
            }
            for (int i = 0; i < fields.length; ++i) {
                if (fields[i] == null) {
                    if (oth.fields[i] != null) {
                        return false;
                    }
                } else {
                    if (!fields[i].equals(oth.fields[i])) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    @Override
    public int hashCode() {
        int result = fields.length;
        for (Object field : fields) {
            if (field != null) {
                result ^= field.hashCode();
            }
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("{");
        for (int i = 0; i < fields.length; ++i) {
            if (i != 0) {
                buffer.append(", ");
            }
            buffer.append(fields[i]);
        }
        buffer.append("}");
        return buffer.toString();
    }

    
  /* Routines for stubbing into Writables */

    public static Object createValue(TypeDescription type, int[] colIndexs) {
        switch (type.getCategory()) {
            case BOOLEAN:
                return new BooleanWritable();
            case BYTE:
                return new ByteWritable();
            case SHORT:
                return new ShortWritable();
            case INT:
                return new IntWritable();
            case LONG:
                return new LongWritable();
            case FLOAT:
                return new FloatWritable();
            case DOUBLE:
                return new DoubleWritable();
            case BINARY:
                return new BytesWritable();
            case CHAR:
            case VARCHAR:
            case STRING:
                return new Text();
            case DATE:
                return new DateWritable();
            case STRUCT: {
                PixelsStruct result = new PixelsStruct(colIndexs.length);
                int c = 0;
                List<TypeDescription> child = type.getChildren();
                for (int index : colIndexs)
                    result.setFieldValue(c++, createValue(child.get(index), colIndexs));

//                for (TypeDescription child : type.getChildren()) {
//                    result.setFieldValue(c++, createValue(child, colIndexs));
//                }
                log.info("result:" + result);
                return result;
            }
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    static class Field implements StructField {
        private final String name;
        private final ObjectInspector inspector;
        private final int offset;

        Field(String name, ObjectInspector inspector, int offset) {
            this.name = name;
            this.inspector = inspector;
            this.offset = offset;
        }

        @Override
        public String getFieldName() {
            return name;
        }

        @Override
        public ObjectInspector getFieldObjectInspector() {
            return inspector;
        }

        @Override
        public int getFieldID() {
            return offset;
        }

        @Override
        public String getFieldComment() {
            return null;
        }
    }

    static class PixelsStructInspector extends SettableStructObjectInspector {
        private List<StructField> fields;

        protected PixelsStructInspector() {
            super();
        }

        PixelsStructInspector(List<StructField> fields) {
            this.fields = fields;
        }

        PixelsStructInspector(StructTypeInfo info) {
            ArrayList<String> fieldNames = info.getAllStructFieldNames();
            ArrayList<TypeInfo> fieldTypes = info.getAllStructFieldTypeInfos();
            fields = new ArrayList<StructField>(fieldNames.size());
            for (int i = 0; i < fieldNames.size(); ++i) {
                fields.add(new Field(fieldNames.get(i),
                        createObjectInspector(fieldTypes.get(i)), i));
            }
        }

        PixelsStructInspector(int columnId, List<PixelsProto.Type> types) {
            PixelsProto.Type type = types.get(columnId);
            int fieldCount = type.getSubtypesCount();
            fields = new ArrayList<StructField>(fieldCount);
            for (int i = 0; i < fieldCount; ++i) {
                int fieldType = type.getSubtypes(i);
                fields.add(new Field(type.getName(),
                        createObjectInspector(fieldType, types), i));
            }
        }

        @Override
        public List<StructField> getAllStructFieldRefs() {
            return fields;
        }

        @Override
        public StructField getStructFieldRef(String s) {
            for (StructField field : fields) {
                if (field.getFieldName().equalsIgnoreCase(s)) {
                    return field;
                }
            }
            return null;
        }

        @Override
        public Object getStructFieldData(Object object, StructField field) {
            if (object == null) {
                return null;
            }
            int offset = ((Field) field).offset;
            PixelsStruct struct = (PixelsStruct) object;
            if (offset >= struct.fields.length) {
                return null;
            }

            return struct.fields[offset];
        }

        @Override
        public List<Object> getStructFieldsDataAsList(Object object) {
            if (object == null) {
                return null;
            }
            PixelsStruct struct = (PixelsStruct) object;
            List<Object> result = new ArrayList<Object>(struct.fields.length);
            for (Object child : struct.fields) {
                result.add(child);
            }
            return result;
        }

        @Override
        public String getTypeName() {
            StringBuilder buffer = new StringBuilder();
            buffer.append("struct<");
            for (int i = 0; i < fields.size(); ++i) {
                StructField field = fields.get(i);
                if (i != 0) {
                    buffer.append(",");
                }
                buffer.append(field.getFieldName());
                buffer.append(":");
                buffer.append(field.getFieldObjectInspector().getTypeName());
            }
            buffer.append(">");
            return buffer.toString();
        }

        @Override
        public ObjectInspector.Category getCategory() {
            return ObjectInspector.Category.STRUCT;
        }

        @Override
        public Object create() {
            return new PixelsStruct(0);
        }

        @Override
        public Object setStructFieldData(Object struct, StructField field,
                                         Object fieldValue) {
            PixelsStruct pixelsStruct = (PixelsStruct) struct;
            int offset = ((Field) field).offset;
            // if the offset is bigger than our current number of fields, grow it
            if (pixelsStruct.getNumFields() <= offset) {
                pixelsStruct.setNumFields(offset + 1);
            }
            pixelsStruct.setFieldValue(offset, fieldValue);
            return struct;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || o.getClass() != getClass()) {
                return false;
            } else if (o == this) {
                return true;
            } else {
                List<StructField> other = ((PixelsStructInspector) o).fields;
                if (other.size() != fields.size()) {
                    return false;
                }
                for (int i = 0; i < fields.size(); ++i) {
                    StructField left = other.get(i);
                    StructField right = fields.get(i);
                    if (!(left.getFieldName().equalsIgnoreCase(right.getFieldName()) &&
                            left.getFieldObjectInspector().equals
                                    (right.getFieldObjectInspector()))) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    static class PixelsMapObjectInspector
            implements MapObjectInspector, SettableMapObjectInspector {
        private ObjectInspector key;
        private ObjectInspector value;

        private PixelsMapObjectInspector() {
            super();
        }

        PixelsMapObjectInspector(MapTypeInfo info) {
            key = createObjectInspector(info.getMapKeyTypeInfo());
            value = createObjectInspector(info.getMapValueTypeInfo());
        }

        PixelsMapObjectInspector(int columnId, List<PixelsProto.Type> types) {
            PixelsProto.Type type = types.get(columnId);
            key = createObjectInspector(type.getSubtypes(0), types);
            value = createObjectInspector(type.getSubtypes(1), types);
        }

        @Override
        public ObjectInspector getMapKeyObjectInspector() {
            return key;
        }

        @Override
        public ObjectInspector getMapValueObjectInspector() {
            return value;
        }

        @Override
        public Object getMapValueElement(Object map, Object key) {
            return ((map == null || key == null) ? null : ((Map) map).get(key));
        }

        @Override
        @SuppressWarnings("unchecked")
        public Map<Object, Object> getMap(Object map) {
            if (map == null) {
                return null;
            }
            return (Map) map;
        }

        @Override
        public int getMapSize(Object map) {
            if (map == null) {
                return -1;
            }
            return ((Map) map).size();
        }

        @Override
        public String getTypeName() {
            return "map<" + key.getTypeName() + "," + value.getTypeName() + ">";
        }

        @Override
        public Category getCategory() {
            return Category.MAP;
        }

        @Override
        public Object create() {
            return new LinkedHashMap<Object, Object>();
        }

        @Override
        public Object put(Object map, Object key, Object value) {
            ((Map) map).put(key, value);
            return map;
        }

        @Override
        public Object remove(Object map, Object key) {
            ((Map) map).remove(key);
            return map;
        }

        @Override
        public Object clear(Object map) {
            ((Map) map).clear();
            return map;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || o.getClass() != getClass()) {
                return false;
            } else if (o == this) {
                return true;
            } else {
                PixelsMapObjectInspector other = (PixelsMapObjectInspector) o;
                return other.key.equals(key) && other.value.equals(value);
            }
        }
    }

    static class PixelsListObjectInspector
            implements ListObjectInspector, SettableListObjectInspector {
        private ObjectInspector child;

        private PixelsListObjectInspector() {
            super();
        }

        PixelsListObjectInspector(ListTypeInfo info) {
            child = createObjectInspector(info.getListElementTypeInfo());
        }

        PixelsListObjectInspector(int columnId, List<PixelsProto.Type> types) {
            PixelsProto.Type type = types.get(columnId);
            child = createObjectInspector(type.getSubtypes(0), types);
        }

        @Override
        public ObjectInspector getListElementObjectInspector() {
            return child;
        }

        @Override
        public Object getListElement(Object list, int i) {
            if (list == null || i < 0 || i >= getListLength(list)) {
                return null;
            }
            return ((List) list).get(i);
        }

        @Override
        public int getListLength(Object list) {
            if (list == null) {
                return -1;
            }
            return ((List) list).size();
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<?> getList(Object list) {
            if (list == null) {
                return null;
            }
            return (List) list;
        }

        @Override
        public String getTypeName() {
            return "array<" + child.getTypeName() + ">";
        }

        @Override
        public ObjectInspector.Category getCategory() {
            return ObjectInspector.Category.LIST;
        }

        @Override
        public Object create(int size) {
            ArrayList<Object> result = new ArrayList<Object>(size);
            for (int i = 0; i < size; ++i) {
                result.add(null);
            }
            return result;
        }

        @Override
        public Object set(Object list, int index, Object element) {
            List l = (List) list;
            for (int i = l.size(); i < index + 1; ++i) {
                l.add(null);
            }
            l.set(index, element);
            return list;
        }

        @Override
        public Object resize(Object list, int newSize) {
            ((ArrayList) list).ensureCapacity(newSize);
            return list;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || o.getClass() != getClass()) {
                return false;
            } else if (o == this) {
                return true;
            } else {
                ObjectInspector other = ((PixelsListObjectInspector) o).child;
                return other.equals(child);
            }
        }
    }

    static public ObjectInspector createObjectInspector(TypeInfo info) {
        switch (info.getCategory()) {
            case PRIMITIVE:
                switch (((PrimitiveTypeInfo) info).getPrimitiveCategory()) {
                    case FLOAT:
                        return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
                    case DOUBLE:
                        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
                    case BOOLEAN:
                        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
                    case BYTE:
                        return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
                    case SHORT:
                        return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
                    case INT:
                        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
                    case LONG:
                        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
                    case BINARY:
                        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
                    case STRING:
                        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
                    case CHAR:
                        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                                (PrimitiveTypeInfo) info);
                    case VARCHAR:
                        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                                (PrimitiveTypeInfo) info);
                    case TIMESTAMP:
                        return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
                    case DATE:
                        return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
                    case DECIMAL:
                        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                                (PrimitiveTypeInfo) info);
                    default:
                        throw new IllegalArgumentException("Unknown primitive type " +
                                ((PrimitiveTypeInfo) info).getPrimitiveCategory());
                }
            case STRUCT:
                return new PixelsStructInspector((StructTypeInfo) info);
            case MAP:
                return new PixelsMapObjectInspector((MapTypeInfo) info);
            case LIST:
                return new PixelsListObjectInspector((ListTypeInfo) info);
            default:
                throw new IllegalArgumentException("Unknown type " +
                        info.getCategory());
        }
    }

    static ObjectInspector createObjectInspector(int columnId,
                                                 List<PixelsProto.Type> types) {
        PixelsProto.Type type = types.get(columnId);
        switch (type.getKind()) {
            case FLOAT:
                return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
            case DOUBLE:
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            case BOOLEAN:
                return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
            case BYTE:
                return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
            case SHORT:
                return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
            case INT:
                return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
            case LONG:
                return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            case BINARY:
                return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
            case STRING:
                return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            case CHAR:
                if (!type.hasMaximumLength()) {
                    throw new UnsupportedOperationException(
                            "Illegal use of char type without length in PIXELS type definition.");
                }
                return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                        TypeInfoFactory.getCharTypeInfo(type.getMaximumLength()));
            case VARCHAR:
                if (!type.hasMaximumLength()) {
                    throw new UnsupportedOperationException(
                            "Illegal use of varchar type without length in PIXELS type definition.");
                }
                return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                        TypeInfoFactory.getVarcharTypeInfo(type.getMaximumLength()));
            case TIMESTAMP:
                return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
            case DATE:
                return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
            case STRUCT:
                return new PixelsStructInspector(columnId, types);
            case MAP:
                return new PixelsMapObjectInspector(columnId, types);
            default:
                throw new UnsupportedOperationException("Unknown type " +
                        type.getKind());
        }
    }

}
