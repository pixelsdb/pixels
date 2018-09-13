package cn.edu.ruc.iir.pixels.common;

import cn.edu.ruc.iir.pixels.common.metadata.ReqParams;
import org.junit.Test;

public class TestReqParams
{
    @Test
    public void test ()
    {
        ReqParams reqParams1 = new ReqParams("getTables");
        //reqParams1.setParam("tableName", "test30g");
        //reqParams1.setParam("schemaName", "test");
        System.out.println(reqParams1);

        ReqParams reqParams = ReqParams.parse(reqParams1.toString());
        System.out.println(reqParams.getAction());
        System.out.println(reqParams.getParam("tableName"));
        System.out.println(reqParams.getParam("schemaName"));
    }
}
