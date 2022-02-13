package io.pixelsdb.pixels.common;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created at: 12/02/2022
 * Author: hank
 */
public class TestLocal
{
    @Test
    public void test() throws IOException
    {
        Storage local = StorageFactory.Instance().getStorage("file:///home/hank/test");
        List<String> paths = local.listPaths("file:///home/hank/");
        for (String path : paths)
        {
            System.out.println(path);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(local.open("file:///home/hank/test")));
        String line;
        while ((line = reader.readLine()) != null)
        {
            System.out.println(line);
        }
    }
}
