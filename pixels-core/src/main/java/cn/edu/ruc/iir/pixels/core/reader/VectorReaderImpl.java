package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public class VectorReaderImpl implements VectorReader
{
    private final PixelsReader fileReader;
    private final ColumnReader[] columnReaders;
    private Chunk[] chunks;
    private PixelsProto.RowGroupFooter[] rowGroupFooters;

    public VectorReaderImpl(PixelsReader fileReader, int[] selectedRowGroups, int[] selectedFields) throws IOException
    {
        this.fileReader = fileReader;
        this.columnReaders = new ColumnReader[selectedFields.length];
        this.rowGroupFooters = new PixelsProto.RowGroupFooter[fileReader.getRowGroupNum()];
        List<TypeDescription> children = fileReader.getSchema().getChildren();

        if (children != null) {
            for (int i = 0; i < selectedFields.length; i++) {
                TypeDescription type = children.get(selectedFields[i]);
                columnReaders[i] = createColumnReader(type);
            }
        }
        for (int i = 0; i < rowGroupFooters.length; i++) {
            rowGroupFooters[i] = null;
        }
        findChunks(selectedRowGroups, selectedFields);
    }

    // find all chunks need to read and sort by offsets
    private void findChunks(int[] selectedRowGroups, int[] selectedFields) throws IOException
    {
        int size = selectedRowGroups.length * selectedFields.length;
        this.chunks = new Chunk[size];
        int index = 0;
        PixelsProto.ColumnChunkIndex columnChunkIndex;
        for (int i = 0; i < selectedRowGroups.length; i++) {
            for (int j = 0; j < selectedFields.length; j++) {
                if (rowGroupFooters[i] == null) {
                    rowGroupFooters[i] = fileReader.readRowGroupFooter(i);
                }
                columnChunkIndex = rowGroupFooters[i].getRowGroupIndexEntry().getColumnChunkIndexEntries(j);
                index++;
                chunks[index] = new Chunk(i, j,
                        columnChunkIndex.getChunkOffset(),
                        columnChunkIndex.getChunkLength());
            }
        }
        // sort chunks
        Arrays.sort(chunks, (o1, o2) -> {
            if (o1.chunkOffset == o2.chunkOffset) {
                return 0;
            }
            return o1.chunkOffset > o2.chunkOffset ? 1 : -1;
        });
    }

    private ColumnReader createColumnReader(TypeDescription schema)
    {
        switch (schema.getCategory())
        {
            case BOOLEAN:
                return new BooleanColumnReader();
            case BYTE:
                return new ByteColumnReader();
            case SHORT:
            case INT:
            case LONG:
                return new IntegerColumnReader();
            case FLOAT:
                return new FloatColumnReader();
            case DOUBLE:
                return new DoubleColumnReader();
            case STRING:
                return new StringColumnReader();
            case CHAR:
                return new CharColumnReader();
            case VARCHAR:
                return new VarcharColumnReader();
            case BINARY:
                return new BinaryColumnReader();
            case TIMESTAMP:
                return new TimestampColumnReader();
            default:
                return null;
        }
    }

    /**
     * Read the next row batch.
     * The size of the batch to read cannot be controlled by the callers.
     * Caller must look at VectorizedRowBatch.size() to know the batch size read.
     *
     * @param batch a row batch object to read into
     * @return were more rows available to read?
     * @throws IOException io exception
     */
    @Override
    public boolean nextBatch(VectorizedRowBatch batch) throws IOException
    {
        return false;
    }

    /**
     * Get the row number of the row that will be returned by the following call to next().
     *
     * @return the row number from 0 th the number of rows in the file
     * @throws IOException io exception
     */
    @Override
    public long getRowNumber() throws IOException
    {
        return 0;
    }

    /**
     * Release the resources associated with the given reader
     *
     * @throws IOException io exception
     */
    @Override
    public void close() throws IOException
    {
    }

    class Chunk
    {
        private int rowGroupId;
        private int fieldId;
        private long chunkOffset;
        private long chunkLength;

        Chunk(int rowGroupId, int fieldId, long chunkOffset, long chunkLength)
        {
            this.rowGroupId = rowGroupId;
            this.fieldId = fieldId;
            this.chunkOffset = chunkOffset;
            this.chunkLength = chunkLength;
        }
    }
}
