package io.pixelsdb.pixels.common.split;


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
