package io.pixelsdb.pixels.presto.split;


public interface Index
{
    /**
     * search viable access pattern for a column set
     *
     * @param columnSet
     * @return
     */
    public AccessPattern search(ColumnSet columnSet);
}
