package cn.edu.ruc.iir.pixels.core.reader;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsRecordReaderOption
{
    private String[] columnNames = null;
    private SearchArgument predicates = null;
    private boolean skipCorruptRecords = false;
    private boolean tolerantSchemaEvolution = true;    // this may lead to column missing due to schema evolution
    private long offset = 0;
    private long length = Long.MAX_VALUE;

    public PixelsRecordReaderOption()
    {}

    public void columnNames(String[] columnNames)
    {
        this.columnNames = columnNames;
    }

    public String[] getColumnNames()
    {
        return columnNames;
    }

    public void predicates(SearchArgument predicates)
    {
        this.predicates = predicates;
    }

    public SearchArgument getPredicates()
    {
        return predicates;
    }

    public void skipCorruptRecords(boolean skipCorruptRecords)
    {
        this.skipCorruptRecords = skipCorruptRecords;
    }

    public boolean isSkipCorruptRecords()
    {
        return skipCorruptRecords;
    }

    public void range(long offset, long length)
    {
        this.offset = offset;
        this.length = length;
    }

    public long getOffset()
    {
        return offset;
    }

    public long getLength()
    {
        return length;
    }

    public void tolerantSchemaEvolution(boolean tolerantSchemaEvolution)
    {
        this.tolerantSchemaEvolution = tolerantSchemaEvolution;
    }

    public boolean isTolerantSchemaEvolution()
    {
        return tolerantSchemaEvolution;
    }
}
