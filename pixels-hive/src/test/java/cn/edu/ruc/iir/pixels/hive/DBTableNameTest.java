package cn.edu.ruc.iir.pixels.hive;

import org.junit.Test;

/**
 * Created at: 19-6-30
 * Author: hank
 */
public class DBTableNameTest
{
    @Test
    public void test ()
    {
        String dirs = "/pixels/db/table/v_0_order";
        dirs = dirs.split(",")[0];
        if (dirs.startsWith("hdfs://"))
        {
            dirs = dirs.substring(7);
        } else if(dirs.startsWith("file://"))
        {
            dirs = dirs.substring(6);
        }
        dirs = dirs.substring(dirs.indexOf('/'));
        System.out.println(dirs);
        String[] tokens = dirs.split("/");
        String[] res = new String[2];
        res[0] = tokens[2];
        res[1] = tokens[3];
        for(String s : res)
        {
            System.out.println(s);
        }
    }
}
