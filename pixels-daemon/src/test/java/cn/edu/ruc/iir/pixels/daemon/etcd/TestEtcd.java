package cn.edu.ruc.iir.pixels.daemon.etcd;

import cn.edu.ruc.iir.pixels.daemon.etcd.util.EtcdUtil;
import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.etcd
 * @ClassName: TestEtcd
 * @Description:
 * @author: tao
 * @date: Create in 2018-09-28 11:40
 **/
public class TestEtcd {

    @Test
    public void testEtcd() throws ExecutionException, InterruptedException {
        // create client
        Client client = Client.builder().endpoints("http://localhost:2379").build();
        KV kvClient = client.getKVClient();

        ByteSequence key = ByteSequence.fromString("test_key");
        ByteSequence value = ByteSequence.fromString("test_value");

        // put the key-value
        kvClient.put(key, value).get();

        // get the CompletableFuture
        CompletableFuture<GetResponse> getFuture = kvClient.get(key);

        // get the value from CompletableFuture
        GetResponse response = getFuture.get();
    }

    @Test
    public void testGetEtcdKey() {
        String address = "10.77.40.236";
        EtcdUtil.ClientInit(null);
        String key = "etcd";
        EtcdUtil.putEtcdKey(key, "hello");
        KeyValue keyValue = EtcdUtil.getEtcdKey(key);
        System.out.println(keyValue.getValue());
    }

}
