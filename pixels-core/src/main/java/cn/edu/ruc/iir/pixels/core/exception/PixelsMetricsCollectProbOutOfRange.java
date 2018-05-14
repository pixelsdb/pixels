package cn.edu.ruc.iir.pixels.core.exception;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsMetricsCollectProbOutOfRange
    extends PixelsReaderException
{
    public PixelsMetricsCollectProbOutOfRange(float prob)
    {
        super("Metrics collect probability " + prob + " is out of range(0.0f-1.0f)");
    }
}
