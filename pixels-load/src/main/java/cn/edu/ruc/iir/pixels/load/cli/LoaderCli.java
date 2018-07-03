package cn.edu.ruc.iir.pixels.load.cli;

import cn.edu.ruc.iir.pixels.common.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Table;
import cn.edu.ruc.iir.pixels.common.utils.DBUtil;
import cn.edu.ruc.iir.pixels.common.utils.FileUtil;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.SchemaDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.TableDao;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.TableElement;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;

/**
 * pixels loader command line tool
 *
 * DDL -s /home/tao/software/data/pixels/DDL.txt -d pixels
 * DDL -s /home/tao/software/data/pixels/test30G_pixels/105/presto_ddl.sql -d pixels
 *
 * LOAD -f pixels -o hdfs://10.77.40.236:9000/pixels/test500G_text/ -d pixels -t testnull_pixels -n 300000
 * LOAD -f orc -o hdfs://10.77.40.236:9000/pixels/test500G_text/ -d pixels -t testnull_orc -n 300000
 */
public class LoaderCli
{
    public static void main(String[] args)
    {
        try {
            Loader loader = null;
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
                    if (executeDDL(parser, dbName, schemaPath))
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
                    argumentParser.addArgument("-f", "--format").required(true)
                            .help("Specify the format of files");
                    argumentParser.addArgument("-d", "--db_name").required(true)
                            .help("specify the name of database");
                    argumentParser.addArgument("-t", "--table_name").required(true)
                            .help("Specify the name of table");
                    argumentParser.addArgument("-o", "--original_data_path").required(true)
                            .help("specify the path of original data");
                    argumentParser.addArgument("-n", "--row_num").required(true)
                            .help("Specify the max number of rows to write in a file");
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

                    String format = namespace.getString("format");
                    String dbName = namespace.getString("db_name");
                    String tableName = namespace.getString("table_name");
                    String originalDataPath = namespace.getString("original_data_path");
                    int rowNum = Integer.parseInt(namespace.getString("row_num"));

                    if (format.equalsIgnoreCase("orc"))
                    {
                        loader = new ORCLoader(originalDataPath, dbName, tableName, rowNum);
                    }
                    if (format.equalsIgnoreCase("pixels"))
                    {
                        loader = new PixelsLoader(originalDataPath, dbName, tableName, rowNum);
                    }

                    if (loader != null)
                    {
                        if (loader.load())
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
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean executeDDL(SqlParser parser, String dbName, String schemaPath)
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

    private static void addColumnsByTableID(List<TableElement> elements, int size, int tableID)
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
