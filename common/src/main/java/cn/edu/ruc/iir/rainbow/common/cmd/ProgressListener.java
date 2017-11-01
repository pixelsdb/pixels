package cn.edu.ruc.iir.rainbow.common.cmd;

public interface ProgressListener
{
    /**
     * percentage is in range of (0, 1).
     * e.g. percentage=0.123 means 12.3%.
     * @param percentage
     */
    public void setPercentage(double percentage);
}
