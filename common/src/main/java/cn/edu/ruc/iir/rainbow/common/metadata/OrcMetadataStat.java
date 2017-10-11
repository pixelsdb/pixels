package cn.edu.ruc.iir.rainbow.common.metadata;

import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.exception.MetadataException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrcMetadataStat implements MetadataStat
{
    private FileSystem fileSystem = null;
    private List<FileStatus> fileStatuses = new ArrayList<>();
    private List<Reader> fileReaders = new ArrayList<>();
    private List<String> fieldNames = new ArrayList<>();
    private int stripeCount = 0;
    private long rowCount = 0;

    double[] columnChunkSizeSums = null;

    // the columns (types) in orc are organized as a tree struct,
    // see https://orc.apache.org/docs/types.html for details.
    // we care the first level children of the root struct.
    // we call these children the root columns.
    private List<Integer> rootColumnIds = null;

    // index is the index in columnIds
    private Map<Integer, Integer> rootColumnIdToIndexMap = new HashMap<>();

    // childId is the ids of the all types in the sub-tree of a root column
    private Map<Integer, Integer> childIdToRootColumnIdMap = new HashMap<>();

    public OrcMetadataStat (String nameNode, int hdfsPort, String dirPath) throws IOException, MetadataException
    {
        Configuration conf = new Configuration();
        // set readahead to 1 can avoid Premature EOF Error of HDFS.
        // although this is a debug level error, it is not elegant.
        conf.set("dfs.datanode.readahead.bytes", "1");
        this.fileSystem = FileSystem.get(URI.create("hdfs://" + nameNode + ":" + hdfsPort), conf);
        Path hdfsDirPath = new Path(dirPath);
        if (! fileSystem.isFile(hdfsDirPath))
        {
            FileStatus[] fileStatuses = fileSystem.listStatus(hdfsDirPath);
            for (FileStatus status : fileStatuses)
            {
                // compatibility for HDFS 1.x
                if (! status.isDir())
                {
                    Reader reader = OrcFile.createReader(status.getPath(),
                            OrcFile.readerOptions(conf));
                    this.fileReaders.add(reader);
                    this.fileStatuses.add(status);
                    this.rowCount += reader.getNumberOfRows();
                    this.stripeCount += reader.getStripes().size();
                }
            }
        }

        if (this.fileReaders.isEmpty())
        {
            throw new MetadataException("fileReaders is empty, path is not a dir.");
        }
        this.fieldNames = this.fileReaders.get(0).getSchema().getFieldNames();


        this.rootColumnIds =
                this.fileReaders.get(0).getFileTail().getFooter().getTypes(0).getSubtypesList();

        for (int i = 0; i < rootColumnIds.size(); ++i)
        {
            this.rootColumnIdToIndexMap.put(rootColumnIds.get(i), i);
            if (i > 0)
            {
                int childNum = rootColumnIds.get(i) - rootColumnIds.get(i-1) - 1;
                int columnId = rootColumnIds.get(i-1);
                for (int j = childNum; j > 0; --j)
                {
                    int childId = rootColumnIds.get(i) - j;
                    this.childIdToRootColumnIdMap.put(childId, columnId);
                }
            }
        }
        int childNum = this.fileReaders.get(0).getFileTail().getFooter().getTypesCount() -
                rootColumnIds.get(rootColumnIds.size()-1) - 1;
        int columnId = rootColumnIds.get(rootColumnIds.size()-1);
        for (int j = 1; j <= childNum; ++j)
        {
            int childId = rootColumnIds.get(rootColumnIds.size()-1) + j;
            this.childIdToRootColumnIdMap.put(childId, columnId);
        }
    }

    /**
     * get the number of rows
     *
     * @return
     */
    @Override
    public long getRowCount()
    {
        return this.rowCount;
    }

    private void readColumnChunkSizes ()
    {
        int columnCount = this.fieldNames.size();
        this.columnChunkSizeSums = new double[columnCount];

        for (int i = 0; i < columnCount; ++i)
        {
            columnChunkSizeSums[i] = 0;
        }

        for (int i = 0; i < this.fileReaders.size(); ++i)
        {
            Reader reader = this.fileReaders.get(i);
            FileStatus status = this.fileStatuses.get(i);
            // we are gonna to pull out the column chunk sizes from stripe footers one by one...
            try (FSDataInputStream inputStream = this.fileSystem.open(status.getPath()))
            {
                for (StripeInformation stripeInfo : reader.getStripes())
                {
                    long offset = stripeInfo.getOffset();
                    long dataLength = stripeInfo.getDataLength();
                    long indexLength = stripeInfo.getIndexLength();
                    long footerLength = stripeInfo.getFooterLength();
                    byte[] buffer = new byte[(int) footerLength];
                    inputStream.readFully(offset + indexLength + dataLength, buffer);
                    OrcProto.StripeFooter footer = OrcProto.StripeFooter.parseFrom(buffer);
                    List<OrcProto.Stream> streams = footer.getStreamsList();
                    for (OrcProto.Stream stream : streams)
                    {
                        int columnId = stream.getColumn();
                        if (this.rootColumnIdToIndexMap.containsKey(columnId))
                        {
                            // this is the stream of a root column
                            int index = this.rootColumnIdToIndexMap.get(columnId);
                            this.columnChunkSizeSums[index] += stream.getLength();

                        } else if (this.childIdToRootColumnIdMap.containsKey(columnId))
                        {
                            // this is the stream of a child column
                            int rootId = this.childIdToRootColumnIdMap.get(columnId);
                            int index = this.rootColumnIdToIndexMap.get(rootId);
                            this.columnChunkSizeSums[index] += stream.getLength();
                        } else
                        {
                            if (columnId != 0)
                            {
                                throw new MetadataException("column id " +
                                        columnId + " not found");
                            }
                        }
                    }
                }
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR,
                        "open orc file error", e);
            } catch (MetadataException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR,
                        "read orc file metadata error", e);
            }
        }
    }

    /**
     * get the average column chunk size of all the row groups (stripes)
     *
     * @return
     */
    @Override
    public double[] getAvgColumnChunkSize()
    {
        int columnCount = this.fieldNames.size();

        if (this.columnChunkSizeSums == null)
        {
            this.readColumnChunkSizes();
        }

        double[] avgs = new double[columnCount];
        for (int i = 0; i < columnCount; ++i)
        {
             avgs[i] = this.columnChunkSizeSums[i] / this.stripeCount;
        }

        return avgs;
    }

    /**
     * get the standard deviation of the column chunk sizes.
     *
     * @param avgSize
     * @return
     */
    @Override
    public double[] getColumnChunkSizeStdDev(double[] avgSize) throws MetadataException
    {
        // TODO: to be supported later
        throw new MetadataException("not supported");
    }

    /**
     * get the field (column) names.
     *
     * @return
     */
    @Override
    public List<String> getFieldNames()
    {
        return new ArrayList<>(this.fieldNames);
    }

    /**
     * get the number of files.
     *
     * @return
     */
    @Override
    public int getFileCount()
    {
        return this.fileReaders.size();
    }

    /**
     * get the number of row groups (stripes)
     * @return
     */
    @Override
    public int getRowGroupCount ()
    {
        return this.stripeCount;
    }

    /**
     * get the average compressed size of the rows in the orc files.
     *
     * @return
     */
    @Override
    public double getRowSize()
    {
        double size = 0;
        for (double columnSize : getAvgColumnChunkSize())
        {
            size += columnSize;
        }
        return size*this.getRowGroupCount()/this.getRowCount();
    }

    /**
     * get the total uncompressed size of the orc files.
     *
     * @return
     */
    @Override
    public long getTotalSize()
    {
        long size = 0;
        for (Reader reader : this.fileReaders)
        {
            // contentLength includes the header ('ORC') length which is 3 bytes.
            size += reader.getContentLength()-3;
        }
        return size;
    }
}
