package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.TypeDescription;

import java.io.InputStream;
import java.sql.Timestamp;

/**
 * pixels timestamp column reader
 * All timestamp values are translated to the specified time zone after read from file.
 *
 * @author guodong
 */
public class TimestampColumnReader
        extends ColumnReader
{
    public TimestampColumnReader(TypeDescription type)
    {
        super(type);
    }

    public Timestamp[] readTimestamps(InputStream input)
    {
        Timestamp[] values = new Timestamp[10];
        long seconds = 1L;
        long nanos = 10L;
        Timestamp ts = new Timestamp(seconds + nanos * 1000);
        return values;
    }
}
