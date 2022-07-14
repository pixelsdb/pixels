package io.pixelsdb.pixels.common.layout;


public interface SplitsIndex
{
    enum IndexType
    {
        INVERTED, COST_BASED
    }

    /**
     * search viable split pattern for a column set
     *
     * @param columnSet
     * @return
     */
    SplitPattern search(ColumnSet columnSet);

    int getVersion();
}
