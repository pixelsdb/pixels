package cn.edu.ruc.iir.pixels.core;

/**
 * pixels
 *
 * @author guodong
 */
public enum PixelsVersion
{
    V1("1.0");

    private String version;

    PixelsVersion(String version)
    {
        this.version = version;
    }

    public String getVersion()
    {
        return this.version;
    }
}
