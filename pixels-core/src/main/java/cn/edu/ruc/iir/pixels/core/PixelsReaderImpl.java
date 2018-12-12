package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.cache.PixelsCacheReader;
import cn.edu.ruc.iir.pixels.common.physical.PhysicalFSReader;
import cn.edu.ruc.iir.pixels.common.physical.PhysicalReaderUtil;
import cn.edu.ruc.iir.pixels.common.utils.Constants;
import cn.edu.ruc.iir.pixels.core.exception.PixelsFileMagicInvalidException;
import cn.edu.ruc.iir.pixels.core.exception.PixelsFileVersionInvalidException;
import cn.edu.ruc.iir.pixels.core.exception.PixelsMetricsCollectProbOutOfRange;
import cn.edu.ruc.iir.pixels.core.exception.PixelsReaderException;
import cn.edu.ruc.iir.pixels.core.reader.PixelsReaderOption;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReaderImpl;
import cn.edu.ruc.iir.pixels.core.utils.PixelsCoreConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static java.util.Objects.requireNonNull;

/**
 * Pixels file reader default implementation
 *
 * @author guodong
 */
@NotThreadSafe
public class PixelsReaderImpl
        implements PixelsReader
{
    private static final Logger LOGGER = LogManager.getLogger(PixelsReaderImpl.class);

    private final TypeDescription fileSchema;
    private final PhysicalFSReader physicalFSReader;
    private final PixelsProto.PostScript postScript;
    private final PixelsProto.Footer footer;
    private final List<PixelsRecordReader> recordReaders;
    private final String metricsDir;
    private final float metricsCollectProb;
    private final boolean enableCache;
    private final List<String> cacheOrder;
    private final PixelsCacheReader pixelsCacheReader;
    private final Random random;

    private PixelsReaderImpl(TypeDescription fileSchema,
                             PhysicalFSReader physicalFSReader,
                             PixelsProto.FileTail fileTail,
                             String metricsDir,
                             float metricsCollectProb,
                             boolean enableCache,
                             List<String> cacheOrder,
                             PixelsCacheReader pixelsCacheReader)
    {
        this.fileSchema = fileSchema;
        this.physicalFSReader = physicalFSReader;
        this.postScript = fileTail.getPostscript();
        this.footer = fileTail.getFooter();
        this.recordReaders = new LinkedList<>();
        this.metricsDir = metricsDir;
        this.metricsCollectProb = metricsCollectProb;
        this.enableCache = enableCache;
        this.cacheOrder = cacheOrder;
        this.pixelsCacheReader = pixelsCacheReader;
        this.random = new Random();
    }

    public static class Builder
    {
        private FileSystem builderFS = null;
        private Path builderPath = null;
        private List<String> builderCacheOrder = null;
        private TypeDescription builderSchema = null;
        private boolean builderEnableCache = false;
        private PixelsCacheReader builderPixelsCacheReader = null;

        private Builder()
        {}

        public Builder setFS(FileSystem fs)
        {
            this.builderFS = requireNonNull(fs);
            return this;
        }

        public Builder setPath(Path path)
        {
            this.builderPath = requireNonNull(path);
            return this;
        }

        public Builder setCacheOrder(List<String> cacheOrder)
        {
            this.builderCacheOrder = cacheOrder;
            return this;
        }

        public Builder setEnableCache(boolean enableCache)
        {
            this.builderEnableCache = enableCache;
            LOGGER.debug("setEnableCache as " + enableCache);
            return this;
        }

        public Builder setPixelsCacheReader(PixelsCacheReader pixelsCacheReader)
        {
            this.builderPixelsCacheReader = pixelsCacheReader;
            return this;
        }

        public PixelsReader build() throws IllegalArgumentException, IOException
        {
            // check arguments
            if (builderFS == null || builderPath == null) {
                throw new IllegalArgumentException("Missing argument to build PixelsReader");
            }
            // get PhysicalFSReader
            PhysicalFSReader fsReader = PhysicalReaderUtil.newPhysicalFSReader(builderFS, builderPath);
            if (fsReader == null) {
                LOGGER.error("Failed to create PhysicalFSReader");
                throw new PixelsReaderException("Failed to create PixelsReader due to error of creating PhysicalFSReader");
            }
            // get FileTail
            long fileLen = fsReader.getFileLength();
            fsReader.seek(fileLen - Long.BYTES);
            long fileTailOffset = fsReader.readLong();
            int fileTailLength = (int) (fileLen - fileTailOffset - Long.BYTES);
            fsReader.seek(fileTailOffset);
            byte[] fileTailBuffer = new byte[fileTailLength];
            fsReader.readFully(fileTailBuffer);
            PixelsProto.FileTail fileTail = PixelsProto.FileTail.parseFrom(fileTailBuffer);

            // check file MAGIC and file version
            PixelsProto.PostScript postScript = fileTail.getPostscript();
            int fileVersion = postScript.getVersion();
            String fileMagic = postScript.getMagic();
            if (!PixelsVersion.matchVersion(fileVersion)) {
                throw new PixelsFileVersionInvalidException(fileVersion);
            }
            if (!fileMagic.contentEquals(Constants.MAGIC)) {
                throw new PixelsFileMagicInvalidException(fileMagic);
            }

            // todo check file schema
            builderSchema = TypeDescription.createSchema(fileTail.getFooter().getTypesList());

            // check metrics file
            PixelsCoreConfig coreConfig = new PixelsCoreConfig();
            String metricsDir = coreConfig.getMetricsDir();
            File file = new File(metricsDir);
            if (!file.isDirectory() || !file.exists()) {
//                throw new PixelsMetricsDirNotFoundException(metricsDir);
            }

            // check metrics collect probability
            float metricCollectProb = coreConfig.getMetricsCollectProb();
            if (metricCollectProb > 1.0f || metricCollectProb < 0.0f) {
                throw new PixelsMetricsCollectProbOutOfRange(metricCollectProb);
            }

            // create a default PixelsReader
            return new PixelsReaderImpl(builderSchema, fsReader, fileTail, metricsDir, metricCollectProb,
                                        builderEnableCache, builderCacheOrder, builderPixelsCacheReader);
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public PixelsProto.RowGroupFooter getRowGroupFooter(int rowGroupId) throws IOException
    {
        long footerOffset = footer.getRowGroupInfos(rowGroupId).getFooterOffset();
        int footerLength = footer.getRowGroupInfos(rowGroupId).getFooterLength();
        byte[] footer = new byte[footerLength];
        physicalFSReader.seek(footerOffset);
        physicalFSReader.readFully(footer);
        return PixelsProto.RowGroupFooter.parseFrom(footer);
    }

    /**
     * Get a <code>PixelsRecordReader</code>
     *
     * @return record reader
     */
    @Override
    public PixelsRecordReader read(PixelsReaderOption option)
    {
        float diceValue = random.nextFloat();
        boolean enableMetrics = false;
        if (diceValue < metricsCollectProb) {
            enableMetrics = true;
        }
        LOGGER.debug("create a recordReader with enableCache as " + enableCache);
        PixelsRecordReader recordReader = new PixelsRecordReaderImpl(physicalFSReader, postScript, footer, option,
                enableMetrics, metricsDir, enableCache, cacheOrder, pixelsCacheReader);
        recordReaders.add(recordReader);
        return recordReader;
    }

    /**
     * Get version of the Pixels file
     *
     * @return version number
     */
    @Override
    public PixelsVersion getFileVersion()
    {
        return PixelsVersion.from(this.postScript.getVersion());
    }

    /**
     * Get the number of rows of the file
     *
     * @return num of rows
     */
    @Override
    public long getNumberOfRows()
    {
        return this.postScript.getNumberOfRows();
    }

    /**
     * Get the compression codec used in this file
     *
     * @return compression codec
     */
    @Override
    public PixelsProto.CompressionKind getCompressionKind()
    {
        return this.postScript.getCompression();
    }

    /**
     * Get the compression block size
     *
     * @return compression block size
     */
    @Override
    public long getCompressionBlockSize()
    {
        return this.postScript.getCompressionBlockSize();
    }

    /**
     * Get the pixel stride
     *
     * @return pixel stride
     */
    @Override
    public long getPixelStride()
    {
        return this.postScript.getPixelStride();
    }

    /**
     * Get the writer's time zone
     *
     * @return time zone
     */
    @Override
    public String getWriterTimeZone()
    {
        return this.postScript.getWriterTimezone();
    }

    /**
     * Get schema of this file
     *
     * @return schema
     */
    @Override
    public TypeDescription getFileSchema()
    {
        return this.fileSchema;
    }

    /**
     * Get the number of row groups in this file
     *
     * @return row group num
     */
    @Override
    public int getRowGroupNum()
    {
        return this.footer.getRowGroupInfosCount();
    }

    /**
     * Get file level statistics of each column
     *
     * @return array of column stat
     */
    @Override
    public List<PixelsProto.ColumnStatistic> getColumnStats()
    {
        return this.footer.getColumnStatsList();
    }

    /**
     * Get file level statistic of the specified column
     *
     * @param columnName column name
     * @return column stat
     */
    @Override
    public PixelsProto.ColumnStatistic getColumnStat(String columnName)
    {
        List<String> fieldNames = fileSchema.getFieldNames();
        int fieldId = fieldNames.indexOf(columnName);
        if (fieldId == -1) {
            return null;
        }
        return this.footer.getColumnStats(fieldId);
    }

    /**
     * Get information of all row groups
     *
     * @return array of row group information
     */
    @Override
    public List<PixelsProto.RowGroupInformation> getRowGroupInfos()
    {
        return this.footer.getRowGroupInfosList();
    }

    /**
     * Get information of specified row group
     *
     * @param rowGroupId row group id
     * @return row group information
     */
    @Override
    public PixelsProto.RowGroupInformation getRowGroupInfo(int rowGroupId)
    {
        if (rowGroupId < 0) {
            return null;
        }
        return this.footer.getRowGroupInfos(rowGroupId);
    }

    /**
     * Get statistics of the specified row group
     *
     * @param rowGroupId row group id
     * @return row group statistics
     */
    @Override
    public PixelsProto.RowGroupStatistic getRowGroupStat(int rowGroupId)
    {
        if (rowGroupId < 0) {
            return null;
        }
        return this.footer.getRowGroupStats(rowGroupId);
    }

    /**
     * Get statistics of all row groups
     * @return row groups statistics
     */
    @Override
    public List<PixelsProto.RowGroupStatistic> getRowGroupStats()
    {
        return this.footer.getRowGroupStatsList();
    }

    public PixelsProto.PostScript getPostScript()
    {
        return postScript;
    }

    public PixelsProto.Footer getFooter()
    {
        return footer;
    }

    /**
     * Cleanup and release resources
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException
    {
        for (PixelsRecordReader recordReader : recordReaders) {
            recordReader.close();
        }
        this.physicalFSReader.close();
    }
}
