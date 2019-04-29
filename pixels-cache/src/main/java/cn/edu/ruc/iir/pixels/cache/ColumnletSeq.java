package cn.edu.ruc.iir.pixels.cache;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ColumnletSeq
{
    private final List<ColumnletId> columnlets;
    public long offset = 0;
    public int length = 0;

    public ColumnletSeq()
    {
        this.columnlets = new ArrayList<>();
    }

    public ColumnletSeq(int capacity)
    {
        this.columnlets = new ArrayList<>(capacity);
    }

    public boolean addColumnlet(ColumnletId columnlet)
    {
        if (length == 0)
        {
            columnlets.add(columnlet);
            offset = columnlet.cacheOffset;
            length += columnlet.cacheLength;
            return true;
        }
        else
        {
            if (columnlet.cacheOffset - offset - length == 0)
            {
                columnlets.add(columnlet);
                length += columnlet.cacheLength;
                return true;
            }
        }
        return false;
    }

    public List<ColumnletId> getSortedChunks()
    {
        columnlets.sort(Comparator.comparingLong(c -> c.cacheOffset));
        return columnlets;
    }
}
