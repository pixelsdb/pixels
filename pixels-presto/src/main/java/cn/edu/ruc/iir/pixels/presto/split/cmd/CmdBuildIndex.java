package cn.edu.ruc.iir.pixels.presto.split.cmd;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.presto.split.builder.PatternBuilder;
import cn.edu.ruc.iir.pixels.presto.split.domain.Split;
import cn.edu.ruc.iir.pixels.presto.split.index.Index;
import cn.edu.ruc.iir.pixels.presto.split.index.IndexFactory;
import cn.edu.ruc.iir.pixels.presto.split.index.Inverted;
import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import com.alibaba.fastjson.JSON;

import java.io.IOException;
import java.util.ArrayList;
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
        String columnSet = params.getProperty("schema.column");
        String split = params.getProperty("split.info");
        Properties results = new Properties(params);
        results.setProperty("success", "false");

        // create the column order.
        List<String> columnOrder = new ArrayList<>();
        String[] columns = columnSet.split(",");
        for (String col : columns) {
            columnOrder.add(col);
        }

        Split splitInfo = JSON.parseObject(split, Split.class);
        int numRowGroupInBlock = splitInfo.getNumRowGroupInBlock();

        Index index = null;
        try {
            index = new Inverted(columnOrder, PatternBuilder.build(splitInfo));
        } catch (IOException e) {
            e.printStackTrace();
        }
        IndexFactory.Instance().cacheIndex(
                ConfigFactory.Instance().getProperty("inverted.index.name"), index);
        results.setProperty("success", "true");

        if (this.receiver != null)

        {
            receiver.progress(1.0);
            receiver.action(results);
        }
    }
}
