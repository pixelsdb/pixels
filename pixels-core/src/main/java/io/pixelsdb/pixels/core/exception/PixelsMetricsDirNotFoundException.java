package io.pixelsdb.pixels.core.exception;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsMetricsDirNotFoundException
        extends PixelsReaderException
{
    private static final long serialVersionUID = 1910875895078923380L;

    public PixelsMetricsDirNotFoundException(String dir)
    {
        super("Metrics dir " + dir + " not found");
    }
}
