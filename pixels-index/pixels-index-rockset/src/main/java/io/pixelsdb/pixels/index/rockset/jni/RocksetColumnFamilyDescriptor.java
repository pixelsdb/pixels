package io.pixelsdb.pixels.index.rockset.jni;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

public final class RocksetColumnFamilyDescriptor
{
    private final byte[] name;
    private final RocksetColumnFamilyOptions options;

    public RocksetColumnFamilyDescriptor(byte[] name,
                                         RocksetColumnFamilyOptions options)
    {
        if (name == null || name.length == 0)
        {
            throw new IllegalArgumentException("ColumnFamily name must not be empty");
        }
        if (options == null)
        {
            throw new IllegalArgumentException("ColumnFamilyOptions must not be null");
        }
        this.name = name;
        this.options = options;
    }

    public byte[] getName()
    {
        return name;
    }

    public RocksetColumnFamilyOptions getOptions()
    {
        return options;
    }
}
