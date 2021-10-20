package io.pixelsdb.pixels.common.layout;


public interface SplitsIndex
{
    /**
     * search viable access pattern for a column set
     *
     * @param columnSet
     * @return
     */
    SplitPattern search(ColumnSet columnSet);

    int getVersion();
}
