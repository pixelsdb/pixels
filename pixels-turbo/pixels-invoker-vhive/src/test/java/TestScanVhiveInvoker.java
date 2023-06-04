import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class TestScanVhiveInvoker
{
    @Test
    public void testScan() throws ExecutionException, InterruptedException
    {
        StorageInfo storageInfo = new StorageInfo(Storage.Scheme.minio, null, null, null);

        ScanOutput output = (ScanOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.SCAN).invoke(Utils.genScanInput(storageInfo, 0)).get();
        System.out.println(JSON.toJSONString(output));
    }
}
