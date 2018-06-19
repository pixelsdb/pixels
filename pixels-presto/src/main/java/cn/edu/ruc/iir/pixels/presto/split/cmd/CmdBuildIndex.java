package cn.edu.ruc.iir.pixels.presto.split.cmd;

import cn.edu.ruc.iir.pixels.common.metadata.domain.Order;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Splits;
import cn.edu.ruc.iir.pixels.presto.split.builder.PatternBuilder;
import cn.edu.ruc.iir.pixels.presto.split.index.Index;
import cn.edu.ruc.iir.pixels.presto.split.index.IndexEntry;
import cn.edu.ruc.iir.pixels.presto.split.index.IndexFactory;
import cn.edu.ruc.iir.pixels.presto.split.index.Inverted;
import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import com.alibaba.fastjson.JSON;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class CmdBuildIndex implements Command {
    private Receiver receiver = null;

    @Override
    public void setReceiver(Receiver receiver) {
        this.receiver = receiver;
    }

    @Override
    public void execute(Properties params) {
        String column = params.getProperty("schema.column");
        String split = params.getProperty("split.info");
        String schemaName = params.getProperty("schema.name");
        String tableName = params.getProperty("table.name");
        Order order = JSON.parseObject(column, Order.class);
        Splits splitInfo = JSON.parseObject(split, Splits.class);

        Properties results = new Properties(params);
        results.setProperty("success", "false");

        List<String> columnOrder = order.getColumnOrder();

        Index index = null;
        try {
            index = new Inverted(columnOrder, PatternBuilder.build(columnOrder, splitInfo));
        } catch (IOException e) {
            e.printStackTrace();
        }
        IndexEntry indexEntry = new IndexEntry(schemaName, tableName);
        IndexFactory.Instance().cacheIndex(indexEntry, index);
        results.setProperty("success", "true");

        if (this.receiver != null)

        {
            receiver.progress(1.0);
            receiver.action(results);
        }
    }
}
