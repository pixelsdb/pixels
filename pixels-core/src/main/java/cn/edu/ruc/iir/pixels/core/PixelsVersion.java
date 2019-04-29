package cn.edu.ruc.iir.pixels.core;

/**
 * pixels
 *
 * @author guodong
 */
public enum PixelsVersion
{
    V1(1);

    private int version;

    PixelsVersion(int version)
    {
        this.version = version;
    }

    public int getVersion()
    {
        return this.version;
    }

    public static PixelsVersion from(int version)
    {
        if (version == 1)
        {
            return PixelsVersion.valueOf("V1");
        }
        throw new IllegalArgumentException("Wrong version.");
    }

    public static boolean matchVersion(int otherVersion)
    {
        return otherVersion == V1.version;
    }

    public static PixelsVersion currentVersion()
    {
        return V1;
    }
}
