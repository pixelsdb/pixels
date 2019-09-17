/*
 * Copyright 2018-2019 PixelsDB.
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
package io.pixelsdb.pixels.hive;

import io.pixelsdb.pixels.hive.common.PixelsStruct;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

/**
 * @Description: A serde class for PIXELS. It transparently passes the object to/from the PIXELS file reader/writer.
 * refers to {@link org.apache.hadoop.hive.ql.io.orc.OrcSerde}
 *
 * <p>
 * @author tao
 * @author hank
 * @date: Create in 2018-12-11 15:29
 * </p>
 **/
public class PixelsSerDe extends AbstractSerDe
{
    private static Logger log = LogManager.getLogger(PixelsSerDe.class);

    private final PixelsRow row = new PixelsRow();
    private ObjectInspector inspector = null;

    @Override
    public void initialize(@Nullable Configuration conf, Properties table) throws SerDeException
    {
        // Read the configuration parameters
        String columnNameProperty = table.getProperty(serdeConstants.LIST_COLUMNS);
        // NOTE: if "columns.types" is missing, all columns will be of String type
        String columnTypeProperty = table.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        final String columnNameDelimiter = table.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? table
                .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);

        // Parse the configuration parameters
        ArrayList<String> columnNames = new ArrayList<>();
        if (columnNameProperty != null && columnNameProperty.length() > 0)
        {
            Collections.addAll(columnNames, columnNameProperty.split(columnNameDelimiter));
        }

        if (columnTypeProperty == null)
        {
            // Default type: all string
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < columnNames.size(); i++)
            {
                if (i > 0)
                {
                    sb.append(":");
                }
                sb.append("string");
            }
            columnTypeProperty = sb.toString();
        }

        ArrayList<TypeInfo> fieldTypes =
                TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        StructTypeInfo rootType = new StructTypeInfo();
        // The source column names for PIXELS serde that will be used in the schema.
        rootType.setAllStructFieldNames(columnNames);
        rootType.setAllStructFieldTypeInfos(fieldTypes);
        inspector = PixelsStruct.createObjectInspector(rootType);
    }

    @Override
    public Class<? extends Writable> getSerializedClass()
    {
        return PixelsRow.class;
    }

    @Override
    public Writable serialize(Object realRow, ObjectInspector objectInspector) throws SerDeException
    {
        row.realRow = realRow;
        row.inspector = inspector;
        return row;
    }

    @Override
    public SerDeStats getSerDeStats()
    {
        return null;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException
    {
        return writable;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException
    {
        return inspector;
    }

    public class PixelsRow implements Writable
    {
        Object realRow;
        ObjectInspector inspector;

        @Override
        public void write(DataOutput dataOutput) throws IOException
        {
            throw new UnsupportedOperationException("can't write the bundle");
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException
        {
            throw new UnsupportedOperationException("can't read the bundle");
        }

        ObjectInspector getInspector()
        {
            return inspector;
        }

        Object getRow()
        {
            return realRow;
        }
    }
}
