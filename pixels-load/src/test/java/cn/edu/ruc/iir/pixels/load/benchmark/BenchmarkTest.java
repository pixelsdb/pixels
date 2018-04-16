package cn.edu.ruc.iir.pixels.load.benchmark;

import cn.edu.ruc.iir.pixels.common.FileUtils;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.TableElement;
import org.junit.Test;

import java.io.*;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class BenchmarkTest {

    private SqlParser parser = new SqlParser();
    String schemaFile = "/home/tao/software/station/bitbucket/pixels/pixels-load/src/main/resources/Test.sql";
    String dataPath = "/home/tao/software/station/bitbucket/pixels/pixels-load/src/main/resources/data/";
    public static final int BUFFER_SIZE = 1024 * 1024 * 32;
    public static final int DATA_MAX = 20;
    private Random random = new Random();

    @Test
    public void testGenerateData() {
        int fileNum = 3;

        String sql = FileUtils.readFileToString(schemaFile);
        CreateTable createTable = (CreateTable) parser.createStatement(sql);
        BufferedWriter bw = null;
        List<TableElement> elements = createTable.getElements();
        int size = elements.size();
        String[] columnTypes = new String[size];
        for (int i = 0; i < size; i++) {
            ColumnDefinition column = (ColumnDefinition) elements.get(i);
            String type = column.getType();
            columnTypes[i] = type;
        }

        String filePath = "";
        int randInt;
        double randDou;
        String randStr;
        for (int i = 1; i <= fileNum; i++) {
            filePath = dataPath + i + ".log";
            try {
                bw = new BufferedWriter(new FileWriter(filePath), BUFFER_SIZE);
            } catch (IOException e) {
                e.printStackTrace();
            }
            for (int j = 0; j < random.nextInt(DATA_MAX) + 1; j++) {
                StringBuilder writeLine = new StringBuilder();
                for (int k = 0; k < size; k++) {
                    String type = columnTypes[k];
                    if (type.equals("int") || type.equals("bigint")) {
                        randInt = random.nextInt(DATA_MAX) + 1;
                        writeLine.append(randInt);
                    } else if (type.equals("double")) {
                        randDou = random.nextDouble();
                        writeLine.append(randDou);
                    } else if (type.equals("varchar")) {
                        randStr = UUID.randomUUID().toString();
                        writeLine.append(randStr);
                    }
                    if (k < size - 1)
                        writeLine.append("\t");
                }
                writeLine.append("\n");
                try {
                    bw.write(writeLine.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                bw.flush();
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
