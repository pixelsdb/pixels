package cn.edu.ruc.iir.pixels.hive;

import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.WritableComparable;

/**
 * An extension for OutputFormats that Output Format needs.
 * todo add other functions if needed
 * @param <V> the row type of the file
 */
public interface PixelsNewOutputFormat<K extends WritableComparable, V> extends HiveOutputFormat<K, V> {

}
