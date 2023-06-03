import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.invoker.vhive.Utils;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class TestPartitionedChainJoinVhiveInvoker
{
    @Test
    public void testPartitionedChainJoin() throws ExecutionException, InterruptedException
    {
        StorageInfo storageInfo = new StorageInfo(Storage.Scheme.minio, null, null, null);

        JoinOutput output = (JoinOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.PARTITIONED_CHAIN_JOIN).invoke(Utils.genPartitionedChainJoinInput(storageInfo)).get();
        System.out.println(JSON.toJSONString(output));
    }
}
