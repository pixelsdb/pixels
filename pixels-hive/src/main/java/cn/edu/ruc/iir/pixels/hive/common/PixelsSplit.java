package cn.edu.ruc.iir.pixels.hive.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplitWithLocationInfo;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * A a section of row groups in an input pixels file.  Returned by
 * {@link InputFormat#getSplits(JobContext)} and passed to
 * {@link InputFormat#createRecordReader(InputSplit, TaskAttemptContext)}.
 * </p>
 * <p>
 * Refers to {@link org.apache.hadoop.mapreduce.lib.input.FileSplit},
 * in which start is replaced by rgStart and rgLen is added.
 * </p>
 * Created at: 19-6-30
 * Author: hank
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class PixelsSplit extends InputSplit implements InputSplitWithLocationInfo
{
    private Path file;
    private int rgStart;
    private int rgLen;
    private long length;
    private boolean cacheEnabled;
    private List<String> cacheOrder;
    private List<String> order;
    // the following two do not need to be serialized.
    private String[] hosts;
    private SplitLocationInfo[] hostInfos;

    public PixelsSplit()
    {
    }

    /**
     * Constructs a split with host information
     *
     * @param file    the file name
     * @param rgStart the index of the first row group in the file to process
     * @param rgLen   the number of row groups in the file for this split to process
     * @param hosts   the list of hosts containing the block, possibly null
     */
    public PixelsSplit(Path file, int rgStart, int rgLen, boolean cacheEnabled,
                       List<String> cacheOrder, List<String> order, long length, String[] hosts)
    {
        assert file != null && cacheOrder != null && order != null;
        this.file = file;
        this.rgStart = rgStart;
        this.rgLen = rgLen;
        this.cacheEnabled = cacheEnabled;
        this.cacheOrder = cacheOrder;
        this.order = order;
        this.length = length;
        this.hosts = hosts;
    }

    /**
     * Constructs a split with host and cached-blocks information
     *
     * @param file          the file name
     * @param rgStart       the index of the first row group in the file to process
     * @param rgLen         the number of row groups in the file for this split to process
     * @param hosts         the list of hosts containing the block
     * @param inMemoryHosts the list of hosts containing the block in memory
     */
    public PixelsSplit(Path file, int rgStart, int rgLen, boolean cacheEnabled,
                       List<String> cacheOrder, List<String> order, long length, String[] hosts,
                       String[] inMemoryHosts)
    {
        this(file, rgStart, rgLen, cacheEnabled, cacheOrder, order, length, hosts);
        hostInfos = new SplitLocationInfo[hosts.length];
        for (int i = 0; i < hosts.length; i++)
        {
            // because N will be tiny, scanning is probably faster than a HashSet
            boolean inMemory = false;
            for (String inMemoryHost : inMemoryHosts)
            {
                if (inMemoryHost.equals(hosts[i]))
                {
                    inMemory = true;
                    break;
                }
            }
            hostInfos[i] = new SplitLocationInfo(hosts[i], inMemory);
        }
    }

    /**
     * The file containing this split's data.
     */
    public Path getPath()
    {
        return file;
    }

    /**
     * The index of the first row group in the file to process.
     */
    public int getRgStart()
    {
        return rgStart;
    }

    /**
     * The number of row groups in the file for this split to process.
     */
    public int getRgLen()
    {
        return rgLen;
    }

    public boolean isCacheEnabled()
    {
        return cacheEnabled;
    }

    public List<String> getCacheOrder()
    {
        return cacheOrder;
    }

    public List<String> getOrder()
    {
        return order;
    }

    /**
     * Get the size of the split, so that the input splits can be sorted by size.
     *
     * @return the number of bytes in the split
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public long getLength() throws IOException
    {
        return length;
    }

    @Override
    public String toString()
    {
        return file + ":" + rgStart + "+" + rgLen;
    }

    ////////////////////////////////////////////
    // Writable methods
    ////////////////////////////////////////////

    @Override
    public void write(DataOutput out) throws IOException
    {
        Text.writeString(out, file.toString());
        out.writeInt(rgStart);
        out.writeInt(rgLen);
        out.writeLong(length);
        out.writeBoolean(cacheEnabled);
        out.writeInt(cacheOrder.size());
        for (String columnlet : cacheOrder)
        {
            Text.writeString(out, columnlet);
        }
        out.writeInt(order.size());
        for (String column : order)
        {
            Text.writeString(out, column);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        file = new Path(Text.readString(in));
        rgStart = in.readInt();
        rgLen = in.readInt();
        length = in.readLong();
        cacheEnabled = in.readBoolean();
        int cacheOrderSize = in.readInt();
        this.cacheOrder = new ArrayList<>(cacheOrderSize);
        for (int i = 0; i < cacheOrderSize; ++i)
        {
            this.cacheOrder.add(Text.readString(in));
        }
        int orderSize = in.readInt();
        this.order = new ArrayList<>(orderSize);
        for (int i = 0; i < orderSize; ++i)
        {
            this.order.add(Text.readString(in));
        }
        hosts = null;
        hostInfos = null;
    }

    @Override
    public String[] getLocations() throws IOException
    {
        if (this.hosts == null)
        {
            return new String[]{};
        } else
        {
            return this.hosts;
        }
    }

    @Override
    @InterfaceStability.Evolving
    public SplitLocationInfo[] getLocationInfo() throws IOException
    {
        return hostInfos;
    }
}
