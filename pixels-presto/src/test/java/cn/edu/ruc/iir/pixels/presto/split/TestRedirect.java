package cn.edu.ruc.iir.pixels.presto.split;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.utils.FileUtil;
import cn.edu.ruc.iir.pixels.presto.impl.PixelsPrestoConfig;
import cn.edu.ruc.iir.pixels.presto.split.cmd.CmdBuildIndex;
import cn.edu.ruc.iir.pixels.presto.split.cmd.CmdRedirect;
import cn.edu.ruc.iir.pixels.presto.split.domain.Split;
import cn.edu.ruc.iir.pixels.presto.split.domain.SplitPattern;
import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import com.alibaba.fastjson.JSON;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TestRedirect {

    @Test
    public void testRedirect() {
        String splitInfo = FileUtil.readFileToString("/home/tao/software/station/bitbucket/pixels/pixels-daemon/src/main/resources/layout_splits.json");
        System.out.println(splitInfo);
        Split split = JSON.parseObject(splitInfo, Split.class);
        List<SplitPattern> splitPatterns = split.getSplitPatterns();

        StringBuilder columnOrder = new StringBuilder();
        for (SplitPattern s : splitPatterns) {
            List<String> cols = s.getAccessedColumns();
            for (String str : cols) {
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
        params.setProperty("column.set", "311,314,315,332");

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

    MetadataService instance = null;

    @Before
    public void init() {
        this.instance = new MetadataService("127.0.0.1", 18888);
    }

    @Test
    public void testIndexRedirect() throws MetadataException {
        String tableName = "test30g_pixels";
        List<Column> columns = instance.getColumns("pixels", tableName);
        List<Layout> layouts = instance.getLayouts("pixels", tableName);
        Layout layout = layouts.get(0);
        String splitInfo = layout.getSplits();
        System.out.println(splitInfo);
        Split split = JSON.parseObject(splitInfo, Split.class);
        // change index to column name
        indexToColumn(split, columns);
        splitInfo = JSON.toJSONString(split);

        List<SplitPattern> splitPatterns = split.getSplitPatterns();

        StringBuilder columnOrder = new StringBuilder();
        for (SplitPattern s : splitPatterns) {
            List<String> cols = s.getAccessedColumns();
            for (String index : cols) {
                if (columnOrder.indexOf(index) < 0) {
                    columnOrder.append(",").append(index);
                }
            }
        }
        System.out.println("Column Order: " + columnOrder.substring(1));

        String schemaColumn = columnOrder.substring(1);
        Properties params = new Properties();
        params.setProperty("schema.column", schemaColumn);
        params.setProperty("split.info", splitInfo);
//        params.setProperty("column.set", "Column_606,Column_610,Column_609,Column_662,Column_663");
        params.setProperty("column.set", "Column_606");
//        params.setProperty("column.set", "Column_706,Column_557,Column_609,Column_662,Column_556,Column_608,Column_612,Column_707,Column_606,Column_120,Column_555,Column_616,Column_610,Column_607");

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

    private void indexToColumn(Split split, List<Column> columns) {
        List<SplitPattern> splitPatterns = split.getSplitPatterns();
        for (SplitPattern s : splitPatterns) {
            List<String> cols = s.getAccessedColumns();
            int num = 0;
            for (String index : cols) {
                Integer i = Integer.valueOf(index);
                String column = columns.get(i).getName();
                cols.set(num, column);
                num++;
            }
        }
    }
}
