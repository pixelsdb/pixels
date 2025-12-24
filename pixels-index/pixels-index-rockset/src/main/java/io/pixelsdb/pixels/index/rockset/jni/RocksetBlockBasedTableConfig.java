package io.pixelsdb.pixels.index.rockset.jni;


public class RocksetBlockBasedTableConfig extends RocksetTableFormatConfig
{
    private Filter filterPolicy;
    private boolean wholeKeyFiltering;
    private long blockSize;
    private RocksetCache blockCache;

    public RocksetBlockBasedTableConfig() {
        this.blockCache = null;
        this.blockSize = 4096L;
        this.filterPolicy = null;
        this.wholeKeyFiltering = true;
    }

    @Override
    protected long newTableFactoryHandle() {
        long var1;
        if (this.filterPolicy != null) {
            var1 = this.filterPolicy.nativeHandle;
        } else {
            var1 = 0L;
        }

        long var3;
        if (this.blockCache != null) {
            var3 = this.blockCache.nativeHandle;
        } else {
            var3 = 0L;
        }

        return newTableFactoryHandle(var3, this.blockSize, var1, this.wholeKeyFiltering);
    }

    public RocksetBlockBasedTableConfig setFilterPolicy(Filter var1) {
        this.filterPolicy = var1;
        return this;
    }
    public RocksetBlockBasedTableConfig setWholeKeyFiltering(boolean var1) {
        this.wholeKeyFiltering = var1;
        return this;
    }

    public RocksetBlockBasedTableConfig setBlockSize(long var1) {
        this.blockSize = var1;
        return this;
    }

    public RocksetBlockBasedTableConfig setBlockCache(RocksetCache var1) {
        this.blockCache = var1;
        return this;
    }

    private static native long newTableFactoryHandle(long var1, long var2, long var3, boolean var4);
}
