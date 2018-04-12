package cn.edu.ruc.iir.pixels.load.cli;

import cn.edu.ruc.iir.pixels.common.DBUtils;
import cn.edu.ruc.iir.pixels.common.FileUtils;
import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.PixelsWriterImpl;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.*;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.BaseDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.ColumnDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.TableDao;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.TableElement;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.load.cli
 * @ClassName: Main
 * @Description: DDL, LOAD
 * <p>
 * DDL -s /home/tao/software/station/bitbucket/pixels/pixels-load/src/main/resources/DDL.txt -d pixels
 * <p>
 * LOAD -p /home/tao/software/station/bitbucket/pixels/pixels-load/src/main/resources/data/ -s /home/tao/software/station/bitbucket/pixels/pixels-load/src/main/resources/Test.sql -f hdfs://presto00:9000/po_compare/test.pxl
 * @author: Tao
 * @date: Create in 2018-04-09 16:00
 **/
public class Main {

    public static void main(String args[]) {
        try {
            Scanner scanner = new Scanner(System.in);
            String inputStr;

            while (true) {
                System.out.print("pixels> ");
                inputStr = scanner.nextLine().trim();

                if (inputStr.isEmpty() || inputStr.equals(";")) {
                    continue;
                }

                if (inputStr.endsWith(";")) {
                    inputStr = inputStr.substring(0, inputStr.length() - 1);
                }

                if (inputStr.equalsIgnoreCase("exit") || inputStr.equalsIgnoreCase("quit") ||
                        inputStr.equalsIgnoreCase("-q")) {
                    System.out.println("Bye.");
                    break;
                }

                if (inputStr.equalsIgnoreCase("help") || inputStr.equalsIgnoreCase("-h")) {
                    System.out.println("Supported commands:\n" +
                            "DDL\n" +
                            "LOAD\n");
                    System.out.println("{command} -h to show the usage of a command.\nexit / quit / -q to exit.\n");
//                    System.out.println("Examples on using these commands: ");
                    continue;
                }

                String command = inputStr.trim().split("\\s+")[0].toUpperCase();

                try {
                    Properties params = new Properties();
                    SqlParser parser = new SqlParser();

                    if (command.equals("DDL")) {
                        ArgumentParser parser1 = ArgumentParsers.newArgumentParser("DDL")
                                .defaultHelp(true);
                        parser1.addArgument("-s", "--schema_file").required(true)
                                .help("specify the path of schema file");
                        parser1.addArgument("-d", "--db_name").required(true)
                                .help("specify the name of database");
                        Namespace namespace1;
                        try {
                            namespace1 = parser1.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                        } catch (ArgumentParserException e) {
                            parser1.handleError(e);
                            continue;
                        }
                        params.setProperty("schema.file", namespace1.getString("schema_file"));

                        String dbName = namespace1.getString("db_name");
                        String schemaFile = namespace1.getString("schema_file");

                        String sql = FileUtils.readFileToString(schemaFile);
                        CreateTable createTable = (CreateTable) parser.createStatement(sql);
                        String tableName = createTable.getName().toString();
                        String insTableSQL = "INSERT INTO TBLS" +
                                "(TBL_NAME, TBL_TYPE, DBS_DB_ID) " +
                                "SELECT '" + tableName + "' as TBL_NAME, '' as TBL_TYPE, " +
                                "DB_ID as DBS_DB_ID from DBS where DB_NAME = '" + dbName + "'";
                        BaseDao baseDao = new ColumnDao();
                        boolean flag = baseDao.update(insTableSQL, null);

                        // COLS
                        if (flag) {
                            // TBLS
                            TableDao tableDao = new TableDao();
                            int tableID = tableDao.getDbIdbyDbName(tableName);
                            List<TableElement> elements = createTable.getElements();
                            int size = elements.size();
                            addColumnsByTableID(elements, size, tableID);

                            System.out.println("Executing command " + command + " successfully");
                        } else {
                            System.out.println("Executing command " + command + " unsuccessfully when adding table info");
                        }
                    }

                    if (command.equals("LOAD")) {
                        ArgumentParser parser1 = ArgumentParsers.newArgumentParser("LOAD")
                                .defaultHelp(true);
                        parser1.addArgument("-s", "--schema_file").required(true)
                                .help("specify the path of schema file");
                        parser1.addArgument("-p", "--data_path").required(true)
                                .help("specify the path of data");
                        parser1.addArgument("-f", "--hdfs_file").required(true)
                                .help("specify the path of file on HDFS");
                        Namespace namespace1;
                        try {
                            namespace1 = parser1.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                        } catch (ArgumentParserException e) {
                            parser1.handleError(e);
                            continue;
                        }
                        params.setProperty("data.path", namespace1.getString("data_path"));
                        params.setProperty("schema.file", namespace1.getString("schema_file"));
                        params.setProperty("hdfs.file", namespace1.getString("hdfs_file"));

                        String dataPath = namespace1.getString("data_path");
                        String filePath = namespace1.getString("hdfs_file");
                        String schemaFile = namespace1.getString("schema_file");

                        String sql = FileUtils.readFileToString(schemaFile);
                        CreateTable createTable = (CreateTable) parser.createStatement(sql);
                        List<TableElement> elements = createTable.getElements();

                        // struct<id:int,x:double,y:double,z:string>
                        String tSchema = "";
                        String prefix = "struct<";
                        StringBuilder suffix = new StringBuilder();

                        int size = elements.size();
                        String[] columnTypes = new String[size];
                        for (int i = 0; i < size; i++) {
                            ColumnDefinition column = (ColumnDefinition) elements.get(i);
                            String name = column.getName().toString();
                            String type = column.getType();
                            if (type.equalsIgnoreCase("varchar"))
                                type = "string";
                            suffix.append(name + ":" + type + ",");
                            columnTypes[i] = type;
                        }
                        tSchema = prefix + suffix.substring(0, suffix.length() - 1) + ">";

                        Configuration conf = new Configuration();
                        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
                        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
                        FileSystem fs = FileSystem.get(URI.create(filePath), conf);
                        TypeDescription schema = TypeDescription.fromString(tSchema);
                        VectorizedRowBatch rowBatch = schema.createRowBatch();
                        ColumnVector[] columnVectors = new ColumnVector[size];

                        for (int i = 0; i < size; i++) {
                            String type = columnTypes[i];
                            if (type.equals("int") || type.equals("bigint")) {
                                LongColumnVector a = (LongColumnVector) rowBatch.cols[i];
                                columnVectors[i] = a;
                            } else if (type.equals("double")) {
                                DoubleColumnVector b = (DoubleColumnVector) rowBatch.cols[i];
                                columnVectors[i] = b;
                            } else if (type.equals("varchar") || type.equals("string")) {
                                BytesColumnVector z = (BytesColumnVector) rowBatch.cols[i];
                                columnVectors[i] = z;
                            }
                        }

                        PixelsWriter pixelsWriter =
                                PixelsWriterImpl.newBuilder()
                                        .setSchema(schema)
                                        .setPixelStride(TestParams.pixelStride)
                                        .setRowGroupSize(TestParams.rowGroupSize)
                                        .setFS(fs)
                                        .setFilePath(new Path(filePath))
                                        .setBlockSize(TestParams.blockSize)
                                        .setReplication(TestParams.blockReplication)
                                        .setBlockPadding(TestParams.blockPadding)
                                        .setEncoding(TestParams.encoding)
                                        .setCompressionBlockSize(TestParams.compressionBlockSize)
                                        .build();

                        Collection<File> files = FileUtils.listFiles(dataPath);
                        BufferedReader br = null;
                        String path = "";
                        String curLine;
                        String splitLine[];
                        for (File f : files) {
                            System.out.println("Loading： " + f.getPath());
                            path = f.getPath();
                            br = new BufferedReader(new FileReader(path));
                            while ((curLine = br.readLine()) != null) {
                                splitLine = curLine.split("\t");

                                int row = rowBatch.size++;
                                for (int j = 0; j < splitLine.length; j++) {
                                    columnVectors[j].add(splitLine[j]);
                                }

                                if (rowBatch.size == rowBatch.getMaxSize()) {
                                    pixelsWriter.addRowBatch(rowBatch);
                                    rowBatch.reset();
                                }
                            }
                        }

                        if (rowBatch.size != 0) {
                            pixelsWriter.addRowBatch(rowBatch);
                            rowBatch.reset();
                        }
                        pixelsWriter.close();
                        System.out.println("Executing command " + command + " successfully");
                    }

                } catch (IllegalArgumentException e) {
                    System.out.println("Executing command " + command + " unsuccessfully");
                    System.out.println(e.getMessage());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void addColumnsByTableID(List<TableElement> elements, int size, int tableID) throws SQLException {
        String insColumnSQL = "";
        String prefix = "INSERT INTO COLS (COL_NAME, COL_TYPE, TBLS_TBL_ID) VALUES ";
        StringBuilder suffix = new StringBuilder();
        DBUtils instance = DBUtils.Instance();
        Connection conn = instance.getConn();
        conn.setAutoCommit(false);
        PreparedStatement pst = instance.getPsmt();

        for (int i = 0; i < size; i++) {
            ColumnDefinition column = (ColumnDefinition) elements.get(i);
            String name = column.getName().toString();
            String type = column.getType();
            suffix.append("('" + name + "', '" + type + "', " + tableID + "),");
            insColumnSQL = prefix + suffix.substring(0, suffix.length() - 1);
            pst.addBatch(insColumnSQL);
            pst.executeBatch();
            conn.commit();
            suffix = new StringBuilder();
        }
        instance.close();
    }
}
