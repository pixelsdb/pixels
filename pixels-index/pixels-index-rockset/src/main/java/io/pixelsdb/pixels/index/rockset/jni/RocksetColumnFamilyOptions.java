package io.pixelsdb.pixels.index.rockset.jni;


public final class RocksetColumnFamilyOptions implements AutoCloseable
{
    private final long nativeHandle;

    public RocksetColumnFamilyOptions()
    {
        this.nativeHandle = newColumnFamilyOptions();
    }

    long handle()
    {
        return nativeHandle;
    }

    public RocksetColumnFamilyOptions setWriteBufferSize(long var1) {
        nativeSetWriteBufferSize(this.nativeHandle, var1);
        return this;
    }

    public RocksetColumnFamilyOptions setMaxWriteBufferNumber(int var1) {
        nativeSetMaxWriteBufferNumber(this.nativeHandle, var1);
        return this;
    }

    public RocksetColumnFamilyOptions setMinWriteBufferNumberToMerge(int var1) {
        nativeSetMinWriteBufferNumberToMerge(this.nativeHandle, var1);
        return this;
    }

    public RocksetColumnFamilyOptions setMemtablePrefixBloomSizeRatio(double var1) {
        nativeSetMemtablePrefixBloomSizeRatio(this.nativeHandle, var1);
        return this;
    }

    public RocksetColumnFamilyOptions setTableFormatConfig(RocksetTableFormatConfig var1) {
        nativeSetTableFactory(this.nativeHandle, var1.newTableFactoryHandle());
        return this;
    }

    public RocksetColumnFamilyOptions setLevel0FileNumCompactionTrigger(int var1) {
        nativeSetLevel0FileNumCompactionTrigger(this.nativeHandle, var1);
        return this;
    }

    public RocksetColumnFamilyOptions setMaxBytesForLevelBase(long var1) {
        nativeSetMaxBytesForLevelBase(this.nativeHandle, var1);
        return this;
    }

    public RocksetColumnFamilyOptions setMaxBytesForLevelMultiplier(double var1) {
        nativeSetMaxBytesForLevelMultiplier(this.nativeHandle, var1);
        return this;
    }

    public RocksetColumnFamilyOptions setTargetFileSizeBase(long var1) {
        nativeSetTargetFileSizeBase(this.nativeHandle, var1);
        return this;
    }

    public RocksetColumnFamilyOptions setTargetFileSizeMultiplier(int var1) {
        nativeSetTargetFileSizeMultiplier(this.nativeHandle, var1);
        return this;
    }

    public RocksetColumnFamilyOptions setCompressionType(RocksetCompressionType var1) {
        nativeSetCompressionType(this.nativeHandle, var1.getValue());
        return this;
    }

    public RocksetColumnFamilyOptions setBottommostCompressionType(RocksetCompressionType var1) {
        nativeSetBottommostCompressionType(this.nativeHandle, var1.getValue());
        return this;
    }

    public RocksetColumnFamilyOptions setCompactionStyle(RocksetCompactionStyle var1) {
        nativeSetCompactionStyle(this.nativeHandle, var1.getValue());
        return this;
    }
    @Override
    public void close()
    {
        disposeInternalJni(this.nativeHandle);
    }

    private static native long newColumnFamilyOptions();
    private static native void nativeSetWriteBufferSize(long var0, long var2) throws IllegalArgumentException;
    private static native void nativeSetMaxWriteBufferNumber(long var0, int var2);
    private static native void nativeSetMinWriteBufferNumberToMerge(long var0, int var2);
    private static native void nativeSetMemtablePrefixBloomSizeRatio(long var0, double var2);
    private static native void nativeSetTableFactory(long var0, long var2);
    private static native void nativeSetLevel0FileNumCompactionTrigger(long var0, int var2);
    private static native void nativeSetMaxBytesForLevelBase(long var0, long var2);
    private static native void nativeSetMaxBytesForLevelMultiplier(long var0, double var2);
    private static native void nativeSetTargetFileSizeBase(long var0, long var2);
    private static native void nativeSetTargetFileSizeMultiplier(long var0, int var2);
    private static native void nativeSetCompressionType(long var0, byte var2);
    private static native void nativeSetBottommostCompressionType(long var0, byte var2);
    private static native void nativeSetCompactionStyle(long var0, byte var2);
    private static native void disposeInternalJni(long var0);
}
