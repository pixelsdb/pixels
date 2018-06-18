package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.common.metadata.Column;
import com.alibaba.fastjson.JSON;
import org.junit.Test;

public class TestJson
{
    @Test
    public void test ()
    {
        Column column = new Column();
        column.setId(1);
        //column.setName("c1");
        column.setType("int");
        System.out.println(JSON.toJSONString(column));
    }
}
