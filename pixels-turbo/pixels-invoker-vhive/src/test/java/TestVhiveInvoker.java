import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import org.junit.Test;

public class TestVhiveInvoker
{
    @Test
    public void testVhive()
    {
        int memorySize = InvokerFactory.Instance().getInvoker(WorkerType.SCAN).getMemoryMB();
        System.out.println(memorySize);
    }
}
