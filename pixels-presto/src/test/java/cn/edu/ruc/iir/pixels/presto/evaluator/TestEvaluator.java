package cn.edu.ruc.iir.pixels.presto.evaluator;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.common.utils.DateUtil;
import org.junit.Test;

import java.io.*;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.evaluator
 * @ClassName: TestEvaluator
 * @Description: Test PrestoEvaluator
 * @author: tao
 * @date: Create in 2018-05-24 14:21
 * java -jar pixels-evaluator.jar /home/presto/opt/pixels/105_workload.text /home/presto/opt/pixels/ /home/presto/opt/presto-server/sbin/drop-caches.sh
 **/
public class TestEvaluator {

    // args: workload_file_path log_dir command
    public static void main(String[] args)
    {
        String workloadFilePath = args[0];
        String logDir = args[1];
        String command = args[2];

        TestEvaluator testEvaluator = new TestEvaluator();
        testEvaluator.testPrestoEvaluator(workloadFilePath, logDir, command);
    }

    private void testPrestoEvaluator(String workloadFilePath, String logDir, String command)
    {
        String testEvalFuc = "pixels"; // pixels, orc
        String testEvalCsv = testEvalFuc + "_duration.csv";
        String tableName = "testnull_" + testEvalFuc;

        ConfigFactory instance = ConfigFactory.Instance();
        Properties properties = new Properties();
//        String user = instance.getProperty("presto.user");
        String password = instance.getProperty("presto.password");
        String ssl = instance.getProperty("presto.ssl");
        String jdbc = instance.getProperty("presto.pixels.jdbc.url");
        if (testEvalFuc.equalsIgnoreCase("orc")) {
            jdbc = instance.getProperty("presto.orc.jdbc.url");
        }

        if (!password.equalsIgnoreCase("null")) {
            properties.setProperty("password", password);
        }
        properties.setProperty("SSL", ssl);

        try (BufferedReader workloadReader = new BufferedReader(new FileReader(workloadFilePath));
             BufferedWriter timeWriter = new BufferedWriter(new FileWriter(logDir + "lite_1000c_" + testEvalCsv))) {
            timeWriter.write("query id,id,duration(ms)\n");
            timeWriter.flush();
            String line;
//            String tableName = "test30g_pixels";
//            String columns = "Column_3, Column_6, Column_100";
            String orderByColumn = null;
            int i = 0;
            String[] lines;
            while ((line = workloadReader.readLine()) != null) {
                lines = line.split("\t");
                properties.setProperty("user", testEvalFuc + "_" + lines[0]);
//                columns = lines[2];
                orderByColumn = getOrderByCol(lines[2]);
                long cost = PrestoEvaluator.execute(jdbc, properties, tableName, lines[2], orderByColumn);
                timeWriter.write(line.split("\t")[0] + "," + i + "," + cost + "\n");
                i++;
                if (i % 10 == 0) {
                    timeWriter.flush();
                    System.out.println(i);
                }
                ProcessBuilder processBuilder = new ProcessBuilder(command);
                Process process = processBuilder.start();
                process.waitFor();
                Thread.sleep(1000);
            }
            timeWriter.flush();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getOrderByCol(String columns) {
        String[] cols = columns.split(",");
        int index = 0;
        int count = 0;
        for (String val : cols) {
            while ((index = columns.indexOf(val, index)) != -1) {
                index = index + val.length();
                count++;
                if (count >= 2) {
                    break;
                }
            }
//            System.out.println("orderByColumn: " + val + ", times: " + count + ",pos: " + index);
            if (count <= 1) {
                System.out.println("orderByColumn: " + val + ", times: " + count + ",pos: " + index);
                return val;
            } else {
                count = 0;
                index = 0;
            }
        }
        return "";
    }

    @Test
    public void testGetOrderByCol() {
        String columns = "QueryDate_,RawQuery,Market,IsBotVNext,IsNormalQuery,Vertical,App\n" +
                "InfoServerName,AppInfoClientName,QueryDate_,DistinctQueryCountVerticalWithinVisi\n" +
                "t";
        getOrderByCol(columns);
    }


    @Test
    public void testGetQuery() {
        String resDir = "/home/tao/software/station/bitbucket/pixels/pixels-presto/src/main/resources/";
        String queryFilePath = resDir + "105_query.text";
        String queryFile = resDir + "105_query_update.text";

        try (BufferedReader queryReader = new BufferedReader(new FileReader(queryFilePath));
             BufferedWriter queryWriter = new BufferedWriter(new FileWriter(queryFile))) {
            String line;
            int i = 0;
            StringBuffer sb = new StringBuffer();
            while ((line = queryReader.readLine()) != null) {
                line = line.replace("\t", " ");
                line = line.trim();
                if (line.length() > 0 && isInteger(line)) {
                    sb.append(i + "\t" + line + "\t");
                } else if (line.startsWith("--")) {
                    continue;
                } else if (line.length() == 0) {
                    sb.append("\n");
                    queryWriter.write(sb.toString());
                    queryWriter.flush();
                    sb = new StringBuffer();
                    i++;
                } else {
                    sb.append(line + " ");
                }
            }
            queryWriter.flush();

            String testEvalFuc = "pixels";
            String tableNmae = "testnull_pixels";
            doPrestoEvaluator(queryFile, tableNmae, testEvalFuc, resDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isInteger(String str) {
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        return pattern.matcher(str).matches();
    }

    public void doPrestoEvaluator(String workloadFilePath, String tableName, String testEvalFuc, String logDir) {
        String testEvalCsv = "_" + testEvalFuc + "_duration.csv";

        ConfigFactory instance = ConfigFactory.Instance();
        Properties properties = new Properties();
        String user = testEvalFuc;
        String password = instance.getProperty("presto.password");
        String ssl = instance.getProperty("presto.ssl");
        String jdbc = instance.getProperty("presto.pixels.jdbc.url");
        if (testEvalFuc.equalsIgnoreCase("orc")) {
            jdbc = instance.getProperty("presto.orc.jdbc.url");
        }

        if (!password.equalsIgnoreCase("null")) {
            properties.setProperty("password", password);
        }
        properties.setProperty("SSL", ssl);

        String time = DateUtil.formatTime(new Date());

        try (BufferedReader workloadReader = new BufferedReader(new FileReader(workloadFilePath));
             BufferedWriter timeWriter = new BufferedWriter(new FileWriter(logDir + time + testEvalCsv))) {
            timeWriter.write("query id,id,duration(ms)\n");
            timeWriter.flush();
            String line;
            int i = 0;
            String[] lines;
            String id = "";
            while ((line = workloadReader.readLine()) != null) {
                lines = line.split("\t");
                properties.setProperty("user", user + "_" + lines[0]);
                id = line.split("\t")[1];
                long cost = PrestoEvaluator.executeSQL(jdbc, properties, lines[2], id);
                timeWriter.write(id + "," + i + "," + cost + "\n");
                i++;
                if (i % 10 == 0) {
                    timeWriter.flush();
                    System.out.println(i);
                }
            }
            timeWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 1. tidy the workload
    // 2. test the workload

    String logDir = "/home/tao/workspace/";
    String workloadPath = "/home/tao/workspace/105_dedup_query.txt";
    String workloadPath_bak = "/home/tao/workspace/105_dedup_query_bak.txt";
    @Test
    public void doTidyWorkload()
    {
        try (BufferedReader workloadReader = new BufferedReader(new FileReader(workloadPath));
             BufferedWriter workloadWriter = new BufferedWriter(new FileWriter(workloadPath_bak))) {
            String line;

            String first = "QueryDate_ >= ";
            String last = "QueryDate_ <= ";

            String begin = "2015-01-01";
            String end = "2015-05-01";

            String tableName = "test_105";
            String new_tableName = "test_105_perf";

            // some different functions between hive aqnd presto
            // INSTR -> STRPOS
            while ((line = workloadReader.readLine()) != null) {
                if(line.contains("QueryDate_"))
                {
                    int first_begin = line.indexOf(first);
                    int first_end = line.indexOf(" AND QueryDate_ <=");
                    String first_content = line.substring(first_begin + first.length() + 1, first_end - 1);

                    System.out.print(first_content + ",");

                    int last_begin = line.indexOf("QueryDate_ <= ");
                    String last_content = line.substring(last_begin + last.length() + 1, last_begin + last.length() + end.length() + 1);
                    System.out.println(last_content);

                    line = line.replace("QueryDate_ >= '" +first_content, "QueryDate_ >= '" + begin).replace("QueryDate_ <= '" + last_content, "QueryDate_ <= '" + end).replace(tableName, new_tableName);
                }
                workloadWriter.write(line + "\n");
            }
            workloadWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void doExecuteWorkload()
    {
        String type = "pixels";
        String csvPrefix = "105_dedup_query"; // or '', default is "pixels_duration.csv"
        doEvaluator(type, workloadPath_bak, logDir, csvPrefix);
    }

    private static void doEvaluator(String type, String workloadFilePath, String logDir, String csvPrefix)
    {
        String testEvalFuc = type; // pixels, orc
        String testEvalCsv = testEvalFuc + "_duration.csv";
        testEvalCsv = csvPrefix != "" ? ("_" + testEvalCsv) : testEvalCsv;

        ConfigFactory instance = ConfigFactory.Instance();
        Properties properties = new Properties();
//        String user = instance.getProperty("presto.user");
        String password = instance.getProperty("presto.password");
        String ssl = instance.getProperty("presto.ssl");
        String jdbc = instance.getProperty("presto.pixels.jdbc.url");
        if (testEvalFuc.equalsIgnoreCase("orc")) {
            jdbc = instance.getProperty("presto.orc.jdbc.url");
        }

        if (!password.equalsIgnoreCase("null")) {
            properties.setProperty("password", password);
        }
        properties.setProperty("SSL", ssl);

        try (BufferedReader workloadReader = new BufferedReader(new FileReader(workloadFilePath));
             BufferedWriter timeWriter = new BufferedWriter(new FileWriter(logDir + csvPrefix + testEvalCsv))) {
            timeWriter.write("query id,id,duration(ms)\n");
            timeWriter.flush();
            String line;
            int i = 0;
            String defaultUser = null;
            while ((line = workloadReader.readLine()) != null) {
                if(!line.contains("SELECT"))
                {
                    defaultUser = line;
                    properties.setProperty("user", testEvalFuc + "_" + defaultUser);
                }
                else
                {
                    long cost = PrestoEvaluator.executeSQL(jdbc, properties, line, defaultUser);
                    timeWriter.write(defaultUser + "," + i + "," + cost + "\n");
                    i++;
                    Thread.sleep(1000);
                    if (i % 10 == 0) {
                        timeWriter.flush();
                        System.out.println(i);
                    }
                }
            }
            timeWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
