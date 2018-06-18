package cn.edu.ruc.iir.pixels.load.benchmark;

import cn.edu.ruc.iir.pixels.common.utils.FileUtil;
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

        String sql = FileUtil.readFileToString(schemaFile);
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

    /**
     * @ClassName: BenchmarkTest
     * @Title: testCreateSchema
     * @Description: 105Columns -> ddl.sql
     * @param:
     * @author: tao
     * @date: 下午5:46 18-5-27
     */
    @Test
    public void testCreateSchema() {
        String schemaFilePath = "/home/tao/software/data/pixels/test30G_pixels/105/105_schema.txt";
        String ddlFilePath = "/home/tao/software/data/pixels/test30G_pixels/105/orc_ddl.sql";
        try (BufferedReader schemaReader = new BufferedReader(new FileReader(schemaFilePath));
             BufferedWriter ddlWriter = new BufferedWriter(new FileWriter(ddlFilePath))) {
            String line;
            StringBuilder ddl_sql = new StringBuilder();
            StringBuilder load_sql = new StringBuilder();
            String prefix = "CREATE EXTERNAL TABLE test500G_orc\n(\n";
            String suffix = "\n)\nSTORED AS ORC\n" +
                    "LOCATION '/pixels/test500G_orc'\n" +
                    "TBLPROPERTIES (\"orc.compress\"=\"NONE\")";
            String[] cols;
            while ((line = schemaReader.readLine()) != null) {
                cols = line.split("\t");
                if(cols[1].equalsIgnoreCase("long"))
                    cols[1] = "bigint";
                ddl_sql.append(cols[0] + " " + cols[1] + ",\n");
            }
            String schema = prefix + ddl_sql.substring(0, ddl_sql.length() - 2) + suffix;
            ddlWriter.write(schema);
            ddlWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
    * @ClassName: BenchmarkTest
    * @Title: testInsertSchema
    * @Description: 105Columns -> load.sql
    * @param:
    * @author: tao
    * @date: 下午10:26 18-5-27
    */
    @Test
    public void testInsertSchema() {
        String schemaFilePath = "/home/tao/software/data/pixels/test30G_pixels/105/105_schema.txt";
        String loadFilePath = "/home/tao/software/data/pixels/test30G_pixels/105/orc_load.sql";
        try (BufferedReader schemaReader = new BufferedReader(new FileReader(schemaFilePath));
             BufferedWriter loadWriter = new BufferedWriter(new FileWriter(loadFilePath))) {
            String line;
            StringBuilder load_sql = new StringBuilder();
            String prefix = "INSERT OVERWRITE TABLE test500G_orc\nSELECT\n";
            String suffix = "\nFROM test500G_parquet";
            String[] cols;
            while ((line = schemaReader.readLine()) != null) {
                cols = line.split("\t");
                if(cols[1].equalsIgnoreCase("long"))
                    cols[1] = "bigint";
                load_sql.append(cols[0] + ",\n");
            }
            String schema = prefix + load_sql.substring(0, load_sql.length() - 2) + suffix;
            loadWriter.write(schema);
            loadWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
