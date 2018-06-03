package cn.edu.ruc.iir.pixels.load.cli;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.common.utils.DBUtils;
import cn.edu.ruc.iir.pixels.common.utils.DateUtil;
import cn.edu.ruc.iir.pixels.common.utils.FileUtils;
import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.PixelsWriterImpl;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.*;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.BaseDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.ColumnDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.TableDao;
import cn.edu.ruc.iir.pixels.presto.impl.FSFactory;
import cn.edu.ruc.iir.pixels.presto.impl.PixelsPrestoConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.TableElement;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.load.cli
 * @ClassName: Main
 * @Description: DDL, LOAD
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
 * LOAD -p hdfs://10.77.40.236:9000/pixels/test500G_text/ -s /home/tao/software/data/pixels/test30G_pixels/105/presto_ddl.sql -f hdfs://presto00:9000/pixels/test500G_pixels/
 * <br> 105 columns
 * @author: Tao
 * @date: Create in 2018-04-09 16:00
 **/
public class Main {
    public static final long KB = 1024L;
    public static final long MB = 1048576L;
    public static final long GB = 1073741824L;
    public static final long TB = 1099511627776L;

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
                        parser1.addArgument("-f", "--hdfs_file_path").required(true)
                                .help("specify the path of files on HDFS");
                        Namespace namespace1;
                        try {
                            namespace1 = parser1.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                        } catch (ArgumentParserException e) {
                            parser1.handleError(e);
                            continue;
                        }
                        params.setProperty("data.path", namespace1.getString("data_path"));
                        params.setProperty("schema.file", namespace1.getString("schema_file"));
                        params.setProperty("hdfs.file.path", namespace1.getString("hdfs_file_path"));

                        String dataPath = namespace1.getString("data_path");
                        String hdfsFile = namespace1.getString("hdfs_file_path");
                        String schemaFile = namespace1.getString("schema_file");

                        if (!hdfsFile.endsWith("/"))
                            hdfsFile += "/";
                        String filePath = hdfsFile + DateUtil.getCurTime() + ".pxl";

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
                            if (type.equalsIgnoreCase("varchar")) {
                                type = "string";
                            }
                            suffix.append(name + ":" + type + ",");
                            columnTypes[i] = type;
                        }
                        tSchema = prefix + suffix.substring(0, suffix.length() - 1) + ">";

                        ConfigFactory instance = ConfigFactory.Instance();
                        String hdfsConfigPath = instance.getProperty("hdfs.config.dir");
                        Configuration hdfsConfig = new Configuration(false);
                        File hdfsConfigDir = new File(hdfsConfigPath);
                        hdfsConfig.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
                        hdfsConfig.set("fs.file.impl", LocalFileSystem.class.getName());
                        if (hdfsConfigDir.exists() && hdfsConfigDir.isDirectory()) {
                            File[] hdfsConfigFiles = hdfsConfigDir.listFiles((file, s) -> s.endsWith("core-site.xml") || s.endsWith("hdfs-site.xml"));
                            if (hdfsConfigFiles != null && hdfsConfigFiles.length == 2) {
                                hdfsConfig.addResource(hdfsConfigFiles[0].toURI().toURL());
                                hdfsConfig.addResource(hdfsConfigFiles[1].toURI().toURL());
                            }
                        } else {
                            System.out.println("can not read hdfs configuration file in pixels connector. hdfs.config.dir=" + hdfsConfigDir);
                        }
                        FileSystem fs = FileSystem.get(URI.create(filePath), hdfsConfig);
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
                            } else if (type.equals("boolean")) {
                                LongColumnVector c = (LongColumnVector) rowBatch.cols[i];
                                columnVectors[i] = c;
                            }
                        }

                        int pixelStride = Integer.parseInt(instance.getProperty("pixel.stride"));
                        int rowGroupSize = Integer.parseInt(instance.getProperty("row.group.size")) * 1024 * 1024;
                        int blockSize = 256 * 1024 * 1024;
                        short blockReplication = 2;
                        boolean blockPadding = true;
                        boolean encoding = true;
                        int compressionBlockSize = 1;

                        PixelsWriter pixelsWriter =
                                PixelsWriterImpl.newBuilder()
                                        .setSchema(schema)

                                        .setPixelStride(pixelStride)
                                        .setRowGroupSize(rowGroupSize)
                                        .setFS(fs)
                                        .setFilePath(new Path(filePath))
                                        .setBlockSize(blockSize)
                                        .setReplication(blockReplication)
                                        .setBlockPadding(blockPadding)
                                        .setEncoding(encoding)
                                        .setCompressionBlockSize(compressionBlockSize)
                                        .build();

                        if (dataPath.contains("hdfs://")) {
                            PixelsPrestoConfig config = new PixelsPrestoConfig().setMetadataServerUri("pixels://presto00:18888")
                                    .setHdfsConfigDir("/home/presto/opt/hadoop-2.7.3/etc/hadoop/");
//                            String hdfsDir = "hdfs://10.77.40.236:9000/pixels/test500G_orc/";
                            FSFactory fsFactory = new FSFactory(config);
                            List<Path> hdfsList = fsFactory.listFiles(dataPath);
                            BufferedReader br = null;
                            FSDataInputStream fsr = null;
                            String curLine;
                            String splitLine[];
                            String fileSize;
                            // set "", start next line to write
                            DecimalFormat df = new DecimalFormat("#.000");
                            int fileS = 0;
                            int fileNum = 1;
                            for (Path path : hdfsList) {
                                System.out.println("Loading： " + path);
                                fsr = fsFactory.getFileSystem().get().open(path);
                                br = new BufferedReader(new InputStreamReader(fsr));
                                while ((curLine = br.readLine()) != null) {
                                    // benchmark exists True or False
                                    curLine = curLine.replace("False", "0").replace("false", "0").replace("True", "1").replace("true", "1");
                                    splitLine = curLine.split("\t");
                                    int row = rowBatch.size++;
                                    for (int j = 0; j < splitLine.length; j++) {
                                        // exists "NULL" to fix
                                        if (splitLine[j].equalsIgnoreCase("\\N")) {
                                            columnVectors[j].add("0");
                                        } else {
                                            columnVectors[j].add(splitLine[j]);
                                        }
                                    }

                                    if (rowBatch.size == rowBatch.getMaxSize()) {
                                        pixelsWriter.addRowBatch(rowBatch);
                                        rowBatch.reset();
                                    }

                                    // at the end of each col, judge the size of the file
                                    fileS += (curLine + "\n").getBytes().length;
                                    fileSize = df.format((double) fileS / blockSize);
                                    if (Double.valueOf(fileSize) >= 1) {
                                        fileNum++;
                                        pixelsWriter.addRowBatch(rowBatch);
                                        rowBatch.reset();
                                        pixelsWriter.close();

                                        filePath = hdfsFile + DateUtil.getCurTime() + ".pxl";
                                        pixelsWriter =
                                                PixelsWriterImpl.newBuilder()
                                                        .setSchema(schema)
                                                        .setPixelStride(pixelStride)
                                                        .setRowGroupSize(rowGroupSize)
                                                        .setFS(fs)
                                                        .setFilePath(new Path(filePath))
                                                        .setBlockSize(blockSize)
                                                        .setReplication(blockReplication)
                                                        .setBlockPadding(blockPadding)
                                                        .setEncoding(encoding)
                                                        .setCompressionBlockSize(compressionBlockSize)
                                                        .build();
                                        fileS = 0;
                                    }
                                }
                                System.out.println("Loading file number: " + fileNum + ", time: " + DateUtil.formatTime(new Date()));
                            }
                            // only one hdfs file
                            if (fileS > 0) {
                                if (rowBatch.size != 0) {
                                    pixelsWriter.addRowBatch(rowBatch);
                                    rowBatch.reset();
                                }
                                pixelsWriter.close();
                            }
                            br.close();
                        } else {

                            Collection<File> files = FileUtils.listFiles(dataPath, true);
                            BufferedReader br = null;
                            String path = "";
                            String curLine;
                            String splitLine[];
                            StringBuilder writeLine = new StringBuilder();
                            String fileSize;
                            // set "", start next line to write
                            DecimalFormat df = new DecimalFormat("#.000");
                            int fileS = 0;
                            int fileNum = 1;
                            for (File f : files) {
                                System.out.println("Loading： " + f.getPath());
                                path = f.getPath();
                                br = new BufferedReader(new FileReader(path));
                                while ((curLine = br.readLine()) != null) {
                                    // benchmark exists True or False
                                    curLine = curLine.replace("False", "0").replace("false", "0").replace("True", "1").replace("true", "1");
                                    splitLine = curLine.split("\t");
                                    int row = rowBatch.size++;
                                    for (int j = 0; j < splitLine.length; j++) {
                                        columnVectors[j].add(splitLine[j]);
                                    }

                                    if (rowBatch.size == rowBatch.getMaxSize()) {
                                        pixelsWriter.addRowBatch(rowBatch);
                                        rowBatch.reset();
                                    }

                                    // at the end of each col, judge the size of the file
                                    fileS += (curLine + "\n").getBytes().length;
                                    fileSize = df.format((double) fileS / blockSize);
                                    if (Double.valueOf(fileSize) >= 1) {
                                        fileNum++;
                                        pixelsWriter.addRowBatch(rowBatch);
                                        rowBatch.reset();
                                        pixelsWriter.close();

                                        filePath = hdfsFile + DateUtil.getCurTime() + ".pxl";
                                        pixelsWriter =
                                                PixelsWriterImpl.newBuilder()
                                                        .setSchema(schema)
                                                        .setPixelStride(pixelStride)
                                                        .setRowGroupSize(rowGroupSize)
                                                        .setFS(fs)
                                                        .setFilePath(new Path(filePath))
                                                        .setBlockSize(blockSize)
                                                        .setReplication(blockReplication)
                                                        .setBlockPadding(blockPadding)
                                                        .setEncoding(encoding)
                                                        .setCompressionBlockSize(compressionBlockSize)
                                                        .build();
                                        fileS = 0;
                                    }
                                }
                                System.out.println("Loading file number: " + fileNum + ", time: " + DateUtil.formatTime(new Date()));
                            }
                            // only one hdfs file
                            if (fileS > 0) {
                                if (rowBatch.size != 0) {
                                    pixelsWriter.addRowBatch(rowBatch);
                                    rowBatch.reset();
                                }
                                pixelsWriter.close();
                            }
                            br.close();
                        }
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
        String prefix = "INSERT INTO COLS (COL_NAME, COL_TYPE, TBLS_TBL_ID) VALUES (?, ?, ?)";
        DBUtils instance = DBUtils.Instance();
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
