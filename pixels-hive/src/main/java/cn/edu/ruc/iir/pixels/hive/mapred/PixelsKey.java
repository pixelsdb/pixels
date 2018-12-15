/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.pixels.hive.mapred;

import cn.edu.ruc.iir.pixels.hive.PixelsStruct;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This type provides a wrapper for PixelsStruct so that it can be sent through
 * the MapReduce shuffle as a key.
 *
 * The user should set the JobConf with orc.mapred.key.type with the type
 * string of the type.
 */
public final class PixelsKey
        implements WritableComparable<PixelsKey>, JobConfigurable {

    public WritableComparable key;

    public PixelsKey(WritableComparable key) {
        this.key = key;
    }

    public PixelsKey() {
        key = null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        key.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        key.readFields(dataInput);
    }

    @Override
    public void configure(JobConf conf) {
        if (key == null) {
            TypeDescription schema =
                    TypeDescription.fromString(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA
                            .getString(conf));
            key = PixelsStruct.createValue(schema);
        }
    }

    @Override
    public int compareTo(PixelsKey o) {
        return key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || key == null) {
            return false;
        } else if (o.getClass() != getClass()) {
            return false;
        } else {
            return key.equals(((PixelsKey) o).key);
        }
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
