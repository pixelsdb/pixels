package io.pixelsdb.pixels.index.rockset.jni;

public enum RocksetCompactionStyle
{
    LEVEL((byte) 0),
    UNIVERSAL((byte) 1),
    FIFO((byte) 2),
    NONE((byte) 3);

    private final byte value;

    RocksetCompactionStyle(byte value)
    {
        this.value = value;
    }

    public byte getValue()
    {
        return value;
    }

    static RocksetCompactionStyle fromValue(byte value)
    {
        for (RocksetCompactionStyle s : values())
        {
            if (s.value == value)
            {
                return s;
            }
        }
        throw new IllegalArgumentException(
                "Unknown value for RocksetCompactionStyle: " + value);
    }
}
