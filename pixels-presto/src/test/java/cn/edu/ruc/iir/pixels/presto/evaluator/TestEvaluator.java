package cn.edu.ruc.iir.pixels.presto.evaluator;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import org.junit.Test;

import java.io.*;
import java.util.Properties;

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

}
