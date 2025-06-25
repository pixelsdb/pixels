/*
 * Copyright 2025 PixelsDB.
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
 * You should have received a_ copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.utils;

import org.junit.Before;
import org.junit.Test;

import com.google.flatbuffers.FlatBufferBuilder;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ByteColumnVector;
import io.pixelsdb.pixels.core.vector.DateColumnVector;
import io.pixelsdb.pixels.core.vector.DecimalColumnVector;
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;
import io.pixelsdb.pixels.core.vector.FloatColumnVector;
import io.pixelsdb.pixels.core.vector.IntColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.LongDecimalColumnVector;
import io.pixelsdb.pixels.core.vector.TimeColumnVector;
import io.pixelsdb.pixels.core.vector.TimestampColumnVector;
import io.pixelsdb.pixels.core.vector.VectorColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

public class TestRowBatchFlat 
{
    public VectorizedRowBatch CreateRowBatch() 
    {
        String schemaStr = "struct<a_:boolean,b_:date,c_:decimal(5,2),d_:double,e_:float,f_:int,g_:long,h_:decimal(30,20),i:string,j_:time,k_:timestamp,l_:vector>";
        TypeDescription schema = TypeDescription.fromString(schemaStr);
        VectorizedRowBatch rowBatch = schema.createRowBatch(VectorizedRowBatch.DEFAULT_SIZE, TypeDescription.Mode.CREATE_INT_VECTOR_FOR_INT);
        ByteColumnVector a_ = (ByteColumnVector) rowBatch.cols[0];               // boolean
        DateColumnVector b_ = (DateColumnVector) rowBatch.cols[1];               // date
        DecimalColumnVector c_ = (DecimalColumnVector) rowBatch.cols[2];         // decimal
        DoubleColumnVector d_ = (DoubleColumnVector) rowBatch.cols[3];           // double
        FloatColumnVector e_ = (FloatColumnVector) rowBatch.cols[4];             // float
        IntColumnVector f_ = (IntColumnVector) rowBatch.cols[5];                 // int
        LongColumnVector g_ = (LongColumnVector) rowBatch.cols[6];               // long
        LongDecimalColumnVector h_ = (LongDecimalColumnVector) rowBatch.cols[7]; // long decimal
        BinaryColumnVector i_ = (BinaryColumnVector) rowBatch.cols[8];           // string
        TimeColumnVector j_ = (TimeColumnVector) rowBatch.cols[9];               // time
        TimestampColumnVector k_ = (TimestampColumnVector) rowBatch.cols[10];    // timestamp
        VectorColumnVector l_ = (VectorColumnVector) rowBatch.cols[11];          // vector
        
        for (int i = 0; i < 10; ++i)
        {
            int row = rowBatch.size++;
            a_.vector[row] = (byte) (i % 2);
            a_.isNull[row] = false;
            b_.isNull[row] = true;
            c_.vector[row] = 10000 + i;
            c_.isNull[row] = false;
            d_.isNull[row] = true;
            e_.vector[row] = 10000 + i;
            e_.isNull[row] = false;
            f_.isNull[row] = true;
            g_.vector[row] = 10000 + i;
            g_.isNull[row] = false;
            h_.isNull[row] = true;
            i_.setVal(row, String.valueOf(i).getBytes());
            i_.isNull[row] = false;
            j_.isNull[row] = true;
            k_.set(row, 1000 * i);
            k_.isNull[row] = false;
            l_.isNull[row] = true;
        }

        for (int i = 10; i < 20; ++i)
        {
            int row = rowBatch.size++;
            a_.isNull[row] = true;
            b_.dates[row] = 1000 + i;
            b_.isNull[row] = false;
            c_.isNull[row] = true;
            d_.vector[row] = 1000 + i;
            d_.isNull[row] = false;
            e_.isNull[row] = true;
            f_.vector[row] = 1000 + i;
            f_.isNull[row] = false;
            g_.isNull[row] = true;
            h_.vector[row << 1] = 0;
            h_.vector[(row << 1) + 1] = 10000 + i;
            h_.isNull[row] = false;
            i_.isNull[row] = true;
            j_.set(row, 1000 * i);
            j_.isNull[row] = false;
            k_.isNull[row] = true;
            l_.setRef(row, new double[] { i + 0.1, i + 0.2 });
            l_.isNull[row] = false;
        }

        for (int i = 20; i < 30; ++i)
        {
            int row = rowBatch.size++;
            a_.vector[row] = (byte) (i % 2);
            a_.isNull[row] = false;
            b_.isNull[row] = true;
            c_.vector[row] = 10000 + i;
            c_.isNull[row] = false;
            d_.isNull[row] = true;
            e_.vector[row] = 10000 + i;
            e_.isNull[row] = false;
            f_.isNull[row] = true;
            g_.vector[row] = 10000 + i;
            g_.isNull[row] = false;
            h_.isNull[row] = true;
            i_.setVal(row, String.valueOf(i).getBytes());
            i_.isNull[row] = false;
            j_.isNull[row] = true;
            k_.set(row, 1000 * i);
            k_.isNull[row] = false;
            l_.isNull[row] = true;
        }

        i_.noNulls = false; // set i_ to have nulls
        return rowBatch;
    }

    @Test
    public void TestRowBatchFlat() 
    {
        VectorizedRowBatch rowBatch = CreateRowBatch();

        System.out.println("Original Row Batch:");
        System.out.println(rowBatch.toString());

        // serialize
        byte[] bytes = rowBatch.serialize();

        // deserialize
        VectorizedRowBatch desRowBatch = VectorizedRowBatch.deserialize(bytes);
        System.out.println("Deserialized Row Batch:");
        System.out.println(desRowBatch.toString());

        System.out.println(rowBatch.toString().equals(desRowBatch.toString()));
    }

}
