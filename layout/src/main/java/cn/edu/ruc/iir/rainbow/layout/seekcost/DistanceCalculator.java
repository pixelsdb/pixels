package cn.edu.ruc.iir.rainbow.layout.seekcost;

import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.common.exception.ColumnOrderException;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hank on 17-4-26.
 */
public class DistanceCalculator
{
    private Map<Column, Integer> columnIndexMap = null;
    private List<Column> columnOrder = null;
    private List<Double> startOffsets = null;
    private List<Double> endOffsets = null;

    private DistanceCalculator () {}

    public DistanceCalculator(List<Column> columnOrder) throws ColumnOrderException
    {
        this.columnIndexMap = new HashMap<>();
        this.columnOrder = new ArrayList<>();
        this.startOffsets = new ArrayList<>();
        this.endOffsets = new ArrayList<>();
        if (columnOrder == null || columnOrder.isEmpty())
        {
            throw new ColumnOrderException("column order is null or empty.");
        }
        double startOffset = 0;
        int i = 0;
        for (Column column : columnOrder)
        {
            Column c1 = column;
            this.columnIndexMap.put(c1, i);
            i++;
            this.columnOrder.add(c1);
            this.startOffsets.add(startOffset);
            this.endOffsets.add(startOffset+column.getSize());
            startOffset += c1.getSize();
        }
    }

    @Override
    public DistanceCalculator clone()
    {
        DistanceCalculator calculator = null;
        try
        {
            calculator = new DistanceCalculator(this.columnOrder);
        } catch (ColumnOrderException e)
        {
            e.printStackTrace();
        }
        return calculator;
    }

    public void insertColumn (Column column, int index)
    {
        if (index < 0 || index > columnOrder.size())
        {
            throw new IndexOutOfBoundsException("index " + index + " out of bound.");
        }
        Column c1 = column;
        this.columnIndexMap.put(c1, index);
        for (int i = index; i < this.columnOrder.size(); ++i)
        {
            this.columnIndexMap.put(this.columnOrder.get(i), i+1);
        }
        this.columnOrder.add(index, c1);
        if (index > 0)
        {
            this.startOffsets.add(index, this.endOffsets.get(index - 1));
            this.endOffsets.add(index, this.endOffsets.get(index - 1) + column.getSize());
        }
        else if (index == 0)
        {
            this.startOffsets.add(0, 0.0);
            this.endOffsets.add(0, column.getSize());
        }

        for (int i = index+1; i < this.columnOrder.size(); ++i)
        {
            this.startOffsets.set(i, this.startOffsets.get(i)+column.getSize());
            this.endOffsets.set(i, this.endOffsets.get(i)+column.getSize());
        }
    }

    public void swapColumn (int index1, int index2)
    {
        if (index1 < 0 || index1 > columnOrder.size())
        {
            throw new IndexOutOfBoundsException("index " + index1 + " out of bound.");
        }
        if (index2 < 0 || index2 > columnOrder.size())
        {
            throw new IndexOutOfBoundsException("index " + index2 + " out of bound.");
        }
        if (index1 >= index2)
        {
            throw new IndexOutOfBoundsException("index1 " + index1 + " is greater than index2 " + index2);
        }
        this.columnIndexMap.put(this.columnOrder.get(index1), index2);
        this.columnIndexMap.put(this.columnOrder.get(index2), index1);
        double delta = this.columnOrder.get(index1).getSize() - this.columnOrder.get(index2).getSize();
        Column c1 = this.columnOrder.get(index1);
        this.columnOrder.set(index1, this.columnOrder.get(index2));
        this.columnOrder.set(index2, c1);

        for (int i = index1+1; i <= index2; ++i)
        {
            this.startOffsets.set(i, this.startOffsets.get(i) - delta);
        }
        for (int i = index1; i < index2; ++i)
        {
            this.endOffsets.set(i, this.endOffsets.get(i) - delta);
        }

        // deal with query column indexes.
    }

    /*
    public TreeSet<Integer> getQueryColumnIndex (Query query)
    {
        return this.queryColumnIndexMap.get(query);
    }*/

    public int getColumnIndex (Column column)
    {
        return this.columnIndexMap.get(column);
    }

    public double calculateDistance (int index1, int index2)
    {
        if (index1 < 0 || index1 > columnOrder.size())
        {
            throw new IndexOutOfBoundsException("index " + index1 + " out of bound.");
        }
        if (index2 < 0 || index2 > columnOrder.size())
        {
            throw new IndexOutOfBoundsException("index " + index2 + " out of bound.");
        }
        if (index1 == index2)
        {
            return 0;
        }
        if (index1 > index2)
        {
            // we only read columns from the first column to the last column in the column order.
            int tmp = index1;
            index1 = index2;
            index2 = tmp;
        }
        //System.out.println(this.startOffsets.size() + ", " + this.endOffsets.size());
        return this.startOffsets.get(index2) - this.endOffsets.get(index1);
    }

    public int deltaIndex (int index, Column column) throws ColumnNotFoundException
    {
        if (this.columnIndexMap.containsKey(column))
        {
            throw new ColumnNotFoundException("column does not exist.");
        }
        return this.columnIndexMap.get(column) - index;
    }

    public double calculateDistance (Column column1, Column column2) throws ColumnNotFoundException
    {
        if (!this.columnIndexMap.containsKey(column1))
        {
            throw new ColumnNotFoundException("column1 does not exist.");
        }
        if (!this.columnIndexMap.containsKey(column2))
        {
            throw new ColumnNotFoundException("column2 does not exist.");
        }
        int index1 = this.columnIndexMap.get(column1);
        int index2 = this.columnIndexMap.get(column2);
        if (index1 == index2)
        {
            return 0;
        }
        if (index1 > index2)
        {
            // we only read columns from the first column to the last column in the column order.
            int tmp = index1;
            index1 = index2;
            index2 = tmp;
        }
        return this.startOffsets.get(index2) - this.endOffsets.get(index1);
    }
}
