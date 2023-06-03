import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.invoker.vhive.Utils;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class TestAggregationVhiveInvoker
{
    @Test
    public void testAggregation() throws ExecutionException, InterruptedException
    {
        StorageInfo storageInfo = new StorageInfo(Storage.Scheme.minio, null, null, null);

        AggregationOutput output = (AggregationOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.AGGREGATION).invoke(Utils.genAggregationInput(storageInfo)).get();
        System.out.println(JSON.toJSONString(output));
    }
}
