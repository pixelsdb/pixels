package cn.edu.ruc.iir.pixels.presto.split;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Order;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Splits;
import cn.edu.ruc.iir.pixels.presto.PixelsColumnHandle;
import cn.edu.ruc.iir.pixels.presto.split.builder.PatternBuilder;
import cn.edu.ruc.iir.pixels.presto.split.cmd.CmdBuildIndex;
import cn.edu.ruc.iir.pixels.presto.split.cmd.CmdRedirect;
import cn.edu.ruc.iir.pixels.presto.split.domain.AccessPattern;
import cn.edu.ruc.iir.pixels.presto.split.domain.ColumnSet;
import cn.edu.ruc.iir.pixels.presto.split.index.IndexEntry;
import cn.edu.ruc.iir.pixels.presto.split.index.IndexFactory;
import cn.edu.ruc.iir.pixels.presto.split.index.Inverted;
import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import com.alibaba.fastjson.JSON;
import com.facebook.presto.spi.PrestoException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_INVERTED_INDEX_ERROR;
import static java.util.stream.Collectors.toSet;

public class TestRedirect {

    MetadataService instance = null;

    @Before
    public void init() {
        this.instance = new MetadataService("127.0.0.1", 18888);
    }

    @Test
    public void testIndexRedirect() throws MetadataException {
        String tableName = "test30g_pixels";
        List<Layout> layouts = instance.getLayouts("pixels", tableName);
        Layout layout = layouts.get(0);
        String splitInfo = layout.getSplits();
        String columnInfo = layout.getOrder();
        System.out.println(splitInfo);
        Splits split = JSON.parseObject(splitInfo, Splits.class);
        Order order = JSON.parseObject(columnInfo, Order.class);

        Properties params = new Properties();
        params.setProperty("schema.name", "pixels");
        params.setProperty("table.name", "test");
        params.setProperty("schema.column", columnInfo);
        params.setProperty("schema.column", columnInfo);
        params.setProperty("split.info", splitInfo);
//        params.setProperty("column.set", "Column_606,Column_610,Column_609,Column_662,Column_663");
//        params.setProperty("column.set", "Column_606");
        params.setProperty("column.set", "Column_706,Column_557,Column_609,Column_662,Column_556,Column_608,Column_612,Column_707,Column_606,Column_120,Column_555,Column_616,Column_610,Column_607");

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

    @Test
    public void testIndexRedirectByColumnSet() throws MetadataException {
        String schemaName = "pixels";
        String tableName = "test30g_pixels";
        List<Layout> layouts = instance.getLayouts(schemaName, tableName);

//        params.setProperty("column.set", "Column_606,Column_610,Column_609,Column_662,Column_663");
//        params.setProperty("column.set", "Column_606");
        String column = "Column_706,Column_557,Column_609,Column_662,Column_556,Column_608,Column_612,Column_707,Column_606,Column_120,Column_555,Column_616,Column_610,Column_607";
        String[] cols = column.split(",");

        Set<String> desiredColumns = new HashSet<>();
        for (String str : cols){
            desiredColumns.add(str);
        }

        Layout curLayout = layouts.get(0);
        int version = curLayout.getVersion();

        // index is exist
        IndexEntry indexEntry = new IndexEntry(schemaName, tableName);
        Inverted index = (Inverted) IndexFactory.Instance().getIndex(indexEntry);
        if (index == null) {
            index = getInverted(curLayout, indexEntry);
        } else {
            int indexVersion = index.getVersion();
            // todo update
            if (indexVersion < version) {
                index = getInverted(curLayout, indexEntry);
            }
        }
        //todo get split size
        ColumnSet columnSet = new ColumnSet();
        for (String col : desiredColumns) {
            columnSet.addColumn(col);
        }

        AccessPattern bestPattern = index.search(columnSet);
        int splitSize = bestPattern.getSplitSize();
    }

    private Inverted getInverted(Layout curLayout, IndexEntry indexEntry) {
        String columnInfo = curLayout.getOrder();
        String splitInfo = curLayout.getSplits();
        Splits split = JSON.parseObject(splitInfo, Splits.class);
        Order order = JSON.parseObject(columnInfo, Order.class);
        List<String> columnOrder = order.getColumnOrder();
        Inverted index;
        try {
            index = new Inverted(columnOrder, PatternBuilder.build(columnOrder, split));
            IndexFactory.Instance().cacheIndex(indexEntry, index);
        } catch (IOException e) {
            throw new PrestoException(PIXELS_INVERTED_INDEX_ERROR, e);
        }
        return index;
    }
}
