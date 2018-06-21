package cn.edu.ruc.iir.pixels.load.cli;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Order;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Table;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.common.utils.DBUtil;
import cn.edu.ruc.iir.pixels.common.utils.DateUtil;
import cn.edu.ruc.iir.pixels.common.utils.FileUtil;
import cn.edu.ruc.iir.pixels.common.utils.StringUtil;
import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.PixelsWriterImpl;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.SchemaDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.TableDao;
import com.alibaba.fastjson.JSON;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.TableElement;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * <p>
 * DDL -s /home/tao/software/data/pixels/DDL.txt -d pixels
 * <p>
 * LOAD -p /home/tao/software/data/pixels/data/ -s /home/tao/software/data/pixels/Test.sql -f hdfs://presto00:9000/po_compare/
 * <br> 1000 columns
 * <p>
 * DDL -s /home/tao/software/data/pixels/test30G_pixels/presto_ddl.sql -d pixels
 * <p>
 * LOAD -p /home/tao/software/data/pixels/test30G_pixels/data/ -s /home/tao/software/data/pixels/test30G_pixels/presto_ddl.sql -f hdfs://presto00:9000/pixels/test30G_pixels/
 * <p>
 * DDL -s /home/tao/software/data/pixels/test30G_pixels/105/presto_ddl.sql -d pixels
 * <p>
 * LOAD -o hdfs://10.77.40.236:9000/pixels/test500G_text/ -d pixels -t testnull_pixels
 * <br> 105 columns
 * @author: Tao
 * @date: Create in 2018-04-09 16:00
 **/
public class Loader
{
    public static void main(String args[]) {
        Loader loader = new Loader();

        try {
            Scanner scanner = new Scanner(System.in);
            String inputStr;

            while (true)
            {
                System.out.print("pixels> ");
                inputStr = scanner.nextLine().trim();

                if (inputStr.isEmpty() || inputStr.equals(";"))
                {
                    continue;
                }

                if (inputStr.endsWith(";"))
                {
                    inputStr = inputStr.substring(0, inputStr.length() - 1);
                }

                if (inputStr.equalsIgnoreCase("exit") || inputStr.equalsIgnoreCase("quit") ||
                        inputStr.equalsIgnoreCase("-q"))
                {
                    System.out.println("Bye.");
                    break;
                }

                if (inputStr.equalsIgnoreCase("help") || inputStr.equalsIgnoreCase("-h"))
                {
                    System.out.println("Supported commands:\n" +
                            "DDL\n" +
                            "LOAD\n");
                    System.out.println("{command} -h to show the usage of a command.\nexit / quit / -q to exit.\n");
                    continue;
                }

                String command = inputStr.trim().split("\\s+")[0].toUpperCase();

                if (command.equals("DDL"))
                {
                    ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("DDL")
                            .defaultHelp(true);
                    argumentParser.addArgument("-s", "--schema_file").required(true)
                            .help("specify the path of schema file");
                    argumentParser.addArgument("-d", "--db_name").required(true)
                            .help("specify the name of database");
                    Namespace namespace;
                    try
                    {
                        namespace = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                    }
                    catch (ArgumentParserException e)
                    {
                        argumentParser.handleError(e);
                        continue;
                    }

                    SqlParser parser = new SqlParser();
                    String dbName = namespace.getString("db_name");
                    String schemaPath = namespace.getString("schema_file");
                    if (loader.executeDDL(parser, dbName, schemaPath))
                    {
                        System.out.println("Executing command " + command + " successfully");
                    }
                    else
                    {
                        System.out.println("Executing command " + command + " unsuccessfully when adding table info");
                    }
                }

                if (command.equals("LOAD"))
                {
                    ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("LOAD")
                            .defaultHelp(true);
                    argumentParser.addArgument("-d", "--db_name").required(true)
                            .help("specify the name of database");
                    argumentParser.addArgument("-t", "--table_name").required(true)
                            .help("Specify the name of table");
                    argumentParser.addArgument("-o", "--original_data_path").required(true)
                            .help("specify the path of original data");
                    Namespace namespace;
                    try
                    {
                        namespace = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                    }
                    catch (ArgumentParserException e)
                    {
                        argumentParser.handleError(e);
                        continue;
                    }

                    String dbName = namespace.getString("db_name");
                    String tableName = namespace.getString("table_name");
                    String originalDataPath = namespace.getString("original_data_path");

                    if (loader.executeLoad(originalDataPath, dbName, tableName))
                    {
                        System.out.println("Executing command " + command + " successfully");
                    }
                    else
                    {
                        System.out.println("Executing command " + command + " unsuccessfully when loading data");
                    }
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean executeDDL(SqlParser parser, String dbName, String schemaPath)
            throws SQLException
    {
        String sql = FileUtil.readFileToString(schemaPath);
        if (sql == null)
        {
            return false;
        }
        CreateTable createTable = (CreateTable) parser.createStatement(sql, new ParsingOptions());
        String tableName = createTable.getName().toString();

        Table table = new Table();
        SchemaDao schemaDao = new SchemaDao();
        Schema schema = schemaDao.getByName(dbName);
        table.setId(-1);
        table.setName(tableName);
        table.setType("");
        table.setSchema(schema);
        TableDao tableDao = new TableDao();
        boolean flag = tableDao.save(table);
        // ODOT-------------------------------------------

        // COLS
        if (flag) {
            // TBLS
            int tableID = tableDao.getByName(tableName).get(0).getId();
            // ODOT-------------------------------------------
            List<TableElement> elements = createTable.getElements();
            int size = elements.size();
            addColumnsByTableID(elements, size, tableID);
            return true;
        } else {
            return false;
        }
    }

    private boolean executeLoad(String originalDataPath, String dbName, String tableName)
            throws IOException, MetadataException
    {
        // init metadata service
        ConfigFactory configFactory = ConfigFactory.Instance();
        String metaHost = configFactory.getProperty("metadata.server.host");
        int metaPort = Integer.parseInt(configFactory.getProperty("metadata.server.port"));
        MetadataService metadataService = new MetadataService(metaHost, metaPort);
        // get columns of the specified table
        List<Column> columns = metadataService.getColumns(dbName, tableName);
        int colSize = columns.size();
        // record original column names and types
        String[] originalColNames = new String[colSize];
        String[] originalColTypes = new String[colSize];
        for (int i = 0; i < colSize; i++)
        {
            originalColNames[i] = columns.get(i).getName();
            originalColTypes[i] = columns.get(i).getType();
        }
        // get the latest layout for writing
        List<Layout> layouts = metadataService.getLayouts(dbName, tableName);
        Layout writingLayout = null;
        int writingLayoutVersion = -1;
        for (Layout layout : layouts)
        {
            if (layout.getPermission() > 0)
            {
                if (layout.getVersion() > writingLayoutVersion)
                {
                    writingLayout = layout;
                    writingLayoutVersion = layout.getVersion();
                }
            }
        }
        // no layouts for writing currently
        if (writingLayout == null)
        {
            return false;
        }
        // get the column order of the latest writing layout
        Order order = JSON.parseObject(writingLayout.getOrder(), Order.class);
        List<String> layoutColumnOrder = order.getColumnOrder();
        // get path of loading
        String loadingDataPath = writingLayout.getOrderPath();
        if (!loadingDataPath.endsWith("/"))
        {
            loadingDataPath += "/";
        }
        // check size consistency
        if (layoutColumnOrder.size() != colSize)
        {
            return false;
        }
        // map the column order of the latest writing layout to the original column order
        int[] orderMapping = new int[colSize];
        List<String> originalColNameList = Arrays.asList(originalColNames);
        for (int i = 0; i < colSize; i++)
        {
            int index=  originalColNameList.indexOf(layoutColumnOrder.get(i));
            if (index >= 0)
            {
                orderMapping[i] = index;
            }
            else
            {
                return false;
            }
        }
        // construct pixels schema based on the column order of the latest writing layout
        StringBuilder pixelsSchemaBuilder = new StringBuilder("struct<");
        for (int i = 0; i < colSize; i++)
        {
            String name = layoutColumnOrder.get(i);
            String type = originalColTypes[orderMapping[i]];
            pixelsSchemaBuilder.append(name).append(":").append(type)
                    .append(",");
        }
        pixelsSchemaBuilder.replace(pixelsSchemaBuilder.length() - 1, pixelsSchemaBuilder.length(), ">");
        TypeDescription pixelsSchema = TypeDescription.fromString(pixelsSchemaBuilder.toString());

        return loadData(originalDataPath, loadingDataPath, pixelsSchema, orderMapping, configFactory);
    }

    private boolean loadData(String originalDataPath, String loadingDataPath, TypeDescription schema, int[] orderMapping,
                             ConfigFactory configFactory)
            throws IOException
    {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create(loadingDataPath), conf);
        VectorizedRowBatch rowBatch = schema.createRowBatch();
        ColumnVector[] columnVectors = rowBatch.cols;
        int pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
        int rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size")) * 1024 * 1024;
        int blockSize = Integer.parseInt(configFactory.getProperty("block.size")) * 1024 * 1024;
        short replication = Short.parseShort(configFactory.getProperty("block.replication"));

        // read original data
        FileStatus[] fileStatuses = fs.listStatus(new Path(originalDataPath));
        List<Path> originalFilePaths = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses)
        {
            if (fileStatus.isFile())
            {
                originalFilePaths.add(fileStatus.getPath());
            }
        }
        BufferedReader reader = null;
        String line;
        String loadingFilePath = loadingDataPath + DateUtil.getCurTime() + ".pxl";
        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(pixelStride)
                .setRowGroupSize(rowGroupSize)
                .setFS(fs)
                .setFilePath(new Path(loadingFilePath))
                .setBlockSize(blockSize)
                .setReplication(replication)
                .setBlockPadding(true)
                .setEncoding(true)
                .setCompressionBlockSize(1)
                .build();
        for (Path originalFilePath : originalFilePaths)
        {
            reader = new BufferedReader(new InputStreamReader(fs.open(originalFilePath)));
            while ((line = reader.readLine()) != null)
            {
                line = StringUtil.replaceAll(line, "false", "0");
                line = StringUtil.replaceAll(line, "False", "0");
                line = StringUtil.replaceAll(line, "true", "1");
                line = StringUtil.replaceAll(line, "True", "1");
                int rowId = rowBatch.size++;
                String[] colsInLine = line.split("\t");
                for (int i = 0; i < columnVectors.length; i++)
                {
                    int valueIdx = orderMapping[i];
                    if (colsInLine[valueIdx].equalsIgnoreCase("\\N"))
                    {
                        columnVectors[i].isNull[rowId] = true;
                    }
                    else
                    {
                        columnVectors[i].add(colsInLine[valueIdx]);
                    }
                }

                if (rowBatch.size >= rowBatch.getMaxSize())
                {
                    if (!pixelsWriter.addRowBatch(rowBatch))
                    {
                        pixelsWriter.close();
                        loadingFilePath = loadingDataPath + DateUtil.getCurTime() + ".pxl";
                        pixelsWriter = PixelsWriterImpl.newBuilder()
                                .setSchema(schema)
                                .setPixelStride(pixelStride)
                                .setRowGroupSize(rowGroupSize)
                                .setFS(fs)
                                .setFilePath(new Path(loadingFilePath))
                                .setBlockSize(blockSize)
                                .setReplication(replication)
                                .setBlockPadding(true)
                                .setEncoding(true)
                                .setCompressionBlockSize(1)
                                .build();
                    }
                    rowBatch.reset();
                }
            }
        }
        if (rowBatch.size != 0)
        {
            pixelsWriter.addRowBatch(rowBatch);
            rowBatch.reset();
        }
        pixelsWriter.close();
        if (reader != null)
        {
            reader.close();
        }

        return true;
    }

    private void addColumnsByTableID(List<TableElement> elements, int size, int tableID)
            throws SQLException
    {
        String prefix = "INSERT INTO COLS (COL_NAME, COL_TYPE, TBLS_TBL_ID) VALUES (?, ?, ?)";
        DBUtil instance = DBUtil.Instance();
        Connection conn = instance.getConnection();
        conn.setAutoCommit(false);
        PreparedStatement pst = conn.prepareStatement(prefix);
        for (int i = 0; i < size; i++) {
            ColumnDefinition column = (ColumnDefinition) elements.get(i);
            String name = column.getName().toString();
            String type = column.getType();
            pst.setString(1, name);
            pst.setString(2, type);
            pst.setInt(3, tableID);
            pst.addBatch();
            pst.executeBatch();
            conn.commit();
        }
        pst.close();
        conn.close();
    }
}
