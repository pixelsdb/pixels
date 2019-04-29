package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsPredicate;

import java.util.Optional;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsReaderOption
{
    private String[] includedCols = new String[0];
    private PixelsPredicate predicate = null;
    private boolean skipCorruptRecords = false;
    private boolean tolerantSchemaEvolution = true;    // this may lead to column missing due to schema evolution
    private int rgStart = 0;
    private int rgLen = -1;     // -1 means reading to the end of the file

    public PixelsReaderOption()
    {
    }

    public void includeCols(String[] columnNames)
    {
        this.includedCols = columnNames;
    }

    public String[] getIncludedCols()
    {
        return includedCols;
    }

    public void predicate(PixelsPredicate predicate)
    {
        this.predicate = predicate;
    }

    public Optional<PixelsPredicate> getPredicate()
    {
        if (predicate == null)
        {
            return Optional.empty();
        }
        return Optional.of(predicate);
    }

    public void skipCorruptRecords(boolean skipCorruptRecords)
    {
        this.skipCorruptRecords = skipCorruptRecords;
    }

    public boolean isSkipCorruptRecords()
    {
        return skipCorruptRecords;
    }

    public void rgRange(int rgStart, int rgLen)
    {
        this.rgStart = rgStart;
        this.rgLen = rgLen;
    }

    public int getRGStart()
    {
        return this.rgStart;
    }

    public int getRGLen()
    {
        return this.rgLen;
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
