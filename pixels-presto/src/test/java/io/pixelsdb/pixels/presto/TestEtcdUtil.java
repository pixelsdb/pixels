package io.pixelsdb.pixels.presto;

import io.pixelsdb.pixels.common.utils.EtcdUtil;
import com.coreos.jetcd.data.KeyValue;

/**
 * Created at: 18-10-14
 * Author: hank
 */
public class TestEtcdUtil
{
    public static void main(String[] args)
    {
        String key = "cache_version";
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        long start = System.currentTimeMillis();
        //EtcdUtil.putEtcdKey(key, "hello world");
        KeyValue keyValue = etcdUtil.getKeyValue(key);
        long end = System.currentTimeMillis();
        System.out.println((end-start));
        if (keyValue != null)
            System.out.println("keyValue is：" + keyValue.getValue().toStringUtf8());
        else
            System.out.println("keyValue is：" + keyValue);
        etcdUtil.getClient().close();
    }

}
