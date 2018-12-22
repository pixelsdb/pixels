/**
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

package cn.edu.ruc.iir.pixels.hive.core;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/**
 * Define the configuration properties that Orc understands.
 */
public enum PixelsConf {
  ROW_INDEX_STRIDE("orc.row.index.stride",
          "hive.exec.orc.default.row.index.stride", 10000,
          "Define the default ORC index stride in number of rows. (Stride is the\n"+
                  " number of rows n index entry represents.)"),
  STRIPE_SIZE("pixels.stripe.size", "hive.exec.orc.default.stripe.size",
      64L * 1024 * 1024,
      "Define the default ORC stripe size, in bytes."),
  BLOCK_SIZE("pixels.block.size", "hive.exec.orc.default.block.size",
      256L * 1024 * 1024,
      "Define the default file system block size for ORC files."),
  BLOCK_REPLICATION("pixels.block.replication", "hive.exec.orc.default.block.size",
          3,
          "Define the default file system block replication for ORC files."),
  BLOCK_PADDING("orc.block.padding", "hive.exec.orc.default.block.padding",
          true,
          "Define whether stripes should be padded to the HDFS block boundaries."),
  ENCODING_STRATEGY("orc.encoding.strategy", "hive.exec.orc.encoding.strategy",
          true,
          "Define the encoding strategy to use while writing data. Changing this\n"+
                  "will only affect the light weight encoding for integers. This\n" +
                  "flag will not change the compression level of higher level\n" +
                  "compression codec (like ZLIB)."),
  COMPRESSION_STRATEGY("orc.compression.strategy",
          "hive.exec.orc.compression.strategy", 1,
          "Define the compression strategy to use while writing data.\n" +
                  "This changes the compression level of higher level compression\n" +
                  "codec (like ZLIB)."),


  WRITE_FORMAT("orc.write.format", "hive.exec.orc.write.format", "0.12",
      "Define the version of the file to write. Possible values are 0.11 and\n"+
          " 0.12. If this parameter is not defined, ORC will use the run\n" +
          " length encoding (RLE) introduced in Hive 0.12."),
  USE_ZEROCOPY("orc.use.zerocopy", "hive.exec.orc.zerocopy", false,
      "Use zerocopy reads with ORC. (This requires Hadoop 2.3 or later.)"),
  SKIP_CORRUPT_DATA("orc.skip.corrupt.data", "hive.exec.orc.skip.corrupt.data",
      false,
      "If ORC reader encounters corrupt data, this value will be used to\n" +
          "determine whether to skip the corrupt data or throw exception.\n" +
          "The default behavior is to throw exception."),
  TOLERATE_MISSING_SCHEMA("orc.tolerate.missing.schema",
      "hive.exec.orc.tolerate.missing.schema",
      true,
      "Writers earlier than HIVE-4243 may have inaccurate schema metadata.\n"
          + "This setting will enable best effort schema evolution rather\n"
          + "than rejecting mismatched schemas"),
  MAX_FILE_LENGTH("orc.max.file.length", "orc.max.file.length", Long.MAX_VALUE,
      "The maximum size of the file to read for finding the file tail. This\n" +
          "is primarily used for streaming ingest to read intermediate\n" +
          "footers while the file is still open"),
  MAPRED_INPUT_SCHEMA("orc.mapred.input.schema", null, null,
      "The schema that the user desires to read. The values are\n" +
      "interpreted using TypeDescription.fromString."),
  MAPRED_SHUFFLE_VALUE_SCHEMA("orc.mapred.map.output.value.schema", null, null,
      "The schema of the MapReduce shuffle value. The values are\n" +
          "interpreted using TypeDescription.fromString."),
  MAPRED_OUTPUT_SCHEMA("orc.mapred.output.schema", null, null,
      "The schema that the user desires to write. The values are\n" +
          "interpreted using TypeDescription.fromString."),
  INCLUDE_COLUMNS("orc.include.columns", "hive.io.file.readcolumn.ids", null,
      "The list of comma separated column ids that should be read with 0\n" +
          "being the first column, 1 being the next, and so on. ."),
  KRYO_SARG("orc.kryo.sarg", "orc.kryo.sarg", null,
      "The kryo and base64 encoded SearchArgument for predicate pushdown."),
  SARG_COLUMNS("orc.sarg.column.names", "org.sarg.column.names", null,
      "The list of column names for the SearchArgument."),
  ;

  private final String attribute;
  private final String hiveConfName;
  private final Object defaultValue;
  private final String description;

  PixelsConf(String attribute,
             String hiveConfName,
             Object defaultValue,
             String description) {
    this.attribute = attribute;
    this.hiveConfName = hiveConfName;
    this.defaultValue = defaultValue;
    this.description = description;
  }

  public String getAttribute() {
    return attribute;
  }

  public String getHiveConfName() {
    return hiveConfName;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public String getDescription() {
    return description;
  }

  private String lookupValue(Properties tbl, Configuration conf) {
    String result = null;
    if (tbl != null) {
      result = tbl.getProperty(attribute);
    }
    if (result == null && conf != null) {
      result = conf.get(attribute);
      if (result == null && hiveConfName != null) {
        result = conf.get(hiveConfName);
      }
    }
    return result;
  }

  public long getLong(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Long.parseLong(value);
    }
    return ((Number) defaultValue).longValue();
  }

  public long getLong(Configuration conf) {
    return getLong(null, conf);
  }

  public void setLong(Configuration conf, long value) {
    conf.setLong(attribute, value);
  }

  public String getString(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    return value == null ? (String) defaultValue : value;
  }

  public String getString(Configuration conf) {
    return getString(null, conf);
  }

  public void setString(Configuration conf, String value) {
    conf.set(attribute, value);
  }

  public boolean getBoolean(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return (Boolean) defaultValue;
  }

  public boolean getBoolean(Configuration conf) {
    return getBoolean(null, conf);
  }

  public void setBoolean(Configuration conf, boolean value) {
    conf.setBoolean(attribute, value);
  }

  public double getDouble(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return ((Number) defaultValue).doubleValue();
  }

  public double getDouble(Configuration conf) {
    return getDouble(null, conf);
  }

  public void setDouble(Configuration conf, double value) {
    conf.setDouble(attribute, value);
  }
}
