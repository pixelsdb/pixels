import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.invoker.vhive.Utils;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class TestPartitionVhiveInvoker
{
    @Test
    public void testOrder() throws ExecutionException, InterruptedException
    {
        StorageInfo storageInfo = new StorageInfo(Storage.Scheme.minio, null, null, null);

        assert Utils.genPartitionInput("order") != null;
        PartitionOutput output = (PartitionOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.PARTITION).invoke(Utils.genPartitionInput("order").apply(storageInfo, 0)).get();
        System.out.println(JSON.toJSONString(output));
    }

    @Test
    public void testLineitem() throws ExecutionException, InterruptedException
    {
        StorageInfo storageInfo = new StorageInfo(Storage.Scheme.minio, null, null, null);

        assert Utils.genPartitionInput("lineitem") != null;
        PartitionOutput output = (PartitionOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.PARTITION).invoke(Utils.genPartitionInput("lineitem").apply(storageInfo, 0)).get();
        System.out.println(JSON.toJSONString(output));
    }
}
