package cn.edu.ruc.iir.pixels.presto.split.cmd;

import cn.edu.ruc.iir.pixels.presto.split.domain.AccessPattern;
import cn.edu.ruc.iir.pixels.presto.split.domain.ColumnSet;
import cn.edu.ruc.iir.pixels.presto.split.index.IndexEntry;
import cn.edu.ruc.iir.pixels.presto.split.index.IndexFactory;
import cn.edu.ruc.iir.pixels.presto.split.index.Inverted;
import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;

import java.util.Properties;

public class CmdRedirect implements Command {
    private Receiver receiver = null;

    @Override
    public void setReceiver(Receiver receiver) {
        this.receiver = receiver;
    }

    @Override
    public void execute(Properties params) {
        String[] columns = params.getProperty("column.set").split(",");
        String schemaName = params.getProperty("schema.name");
        String tableName = params.getProperty("table.name");

        ColumnSet columnSet = new ColumnSet();
        for (String column : columns) {
            columnSet.addColumn(column);
        }

        IndexEntry indexEntry = new IndexEntry(schemaName, tableName);
        Inverted index = (Inverted) IndexFactory.Instance().getIndex(indexEntry);
        AccessPattern bestPattern = index.search(columnSet);

        Properties results = new Properties(params);
        results.setProperty("success", "true");
        results.setProperty("access.pattern", bestPattern.toString());

        if (receiver != null) {
            receiver.progress(1.0);
            receiver.action(results);
        }
    }
}
