package cn.edu.ruc.iir.pixels.presto.split;

import cn.edu.ruc.iir.pixels.common.utils.FileUtils;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.split.Split;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.split.SplitPattern;
import cn.edu.ruc.iir.pixels.presto.split.cmd.CmdBuildIndex;
import cn.edu.ruc.iir.pixels.presto.split.cmd.CmdRedirect;
import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import com.alibaba.fastjson.JSON;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

public class TestRedirect {

    @Test
    public void testRedirect() {
        String splitInfo = FileUtils.readFileToString("/home/tao/software/station/bitbucket/pixels/pixels-daemon/src/main/resources/layouts_layout_split.json");
        System.out.println(splitInfo);
        Split split = JSON.parseObject(splitInfo, Split.class);
        List<SplitPattern> splitPatterns = split.getSplitPatterns();

        StringBuilder columnOrder = new StringBuilder();
        for (SplitPattern s : splitPatterns) {
            String cols = s.getAccessedColumns();
            String[] colSplits = cols.split(",");
            for (String str : colSplits) {
                if (columnOrder.indexOf(str) < 0) {
                    columnOrder.append(",").append(str);
                }
            }
        }
        System.out.println("Column Order: " + columnOrder.substring(1));

        String schemaColumn = columnOrder.append(",Column_0,Column_1,Column_2,Column_3").substring(1);
        Properties params = new Properties();
        params.setProperty("schema.column", schemaColumn);
        params.setProperty("split.info", splitInfo);
//        params.setProperty("column.set", "Column_0,Column_1,Column_2");
        params.setProperty("column.set", "Column_311,Column_332,Column_361");

        Command command = new CmdBuildIndex();
        command.execute(params);

        Receiver receiver = new Receiver() {
            @Override
            public void progress(double percentage) {
                System.out.println(percentage);
            }

            @Override
            public void action(Properties results) {
                System.out.println("=====Achieved Split Info=====");
                System.out.println(results.getProperty("access.pattern"));
            }
        };

        Command command1 = new CmdRedirect();
        command1.setReceiver(receiver);
        command1.execute(params);
    }
}
