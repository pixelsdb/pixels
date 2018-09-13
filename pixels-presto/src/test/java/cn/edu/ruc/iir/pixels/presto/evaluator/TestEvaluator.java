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
 * This shall be run under root user to execute cache cleaning commands
 * java -jar pixels-evaluator.jar pixels /home/presto/opt/pixels/105_workload.text /home/presto/opt/pixels/pixels_duration.csv /home/presto/opt/presto-server/sbin/drop-caches.sh
 * java -jar pixels-evaluator.jar orc /home/presto/opt/pixels/105_workload.text /home/presto/opt/pixels/orc_duration.csv /home/presto/opt/presto-server/sbin/drop-caches.sh
 **/
public class TestEvaluator {

    // args: workload_file_path log_dir command
    public static void main(String[] args)
    {
        String type = args[0];
        String workloadFilePath = args[1];
        String logFile = args[2];
        String command = args[3];

        TestEvaluator testEvaluator = new TestEvaluator();
        testEvaluator.testPrestoEvaluator(type, workloadFilePath, logFile, command);
    }

    private void testPrestoEvaluator(String type, String workloadFilePath, String logFile, String command)
    {
        String testEvalCsv = type + "_duration.csv";
        String tableName = "testnull_" + type;

        ConfigFactory instance = ConfigFactory.Instance();
        Properties properties = new Properties();
//        String user = instance.getProperty("presto.user");
        String password = instance.getProperty("presto.password");
        String ssl = instance.getProperty("presto.ssl");
        String jdbc = instance.getProperty("presto.pixels.jdbc.url");
        if (type.equalsIgnoreCase("orc")) {
            jdbc = instance.getProperty("presto.orc.jdbc.url");
        }
        if (!password.equalsIgnoreCase("null")) {
            properties.setProperty("password", password);
        }
        properties.setProperty("SSL", ssl);

        try (BufferedReader workloadReader = new BufferedReader(new FileReader(workloadFilePath));
             BufferedWriter timeWriter = new BufferedWriter(new FileWriter(logFile))) {
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
                properties.setProperty("user", type + "_" + lines[0]);
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
        }
        catch (Exception e) {
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
