package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.stats.StatsRecorder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

/**
 * pixels
 *
 * @author guodong
 */
public abstract class BaseColumnWriter implements ColumnWriter
{
    private final TypeDescription type;
    private StatsRecorder fileStatistics;


    public BaseColumnWriter(TypeDescription type)
    {
        this.type = type;
    }

    @Override
    public void writeBatch(ColumnVector vector)
    {

    }
}
