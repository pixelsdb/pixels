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
 **/
public class TestEvaluator {

    /**
     * @ClassName: TestEvaluator
     * @Title: testPrestoEvaluator
     * @Description:
     * @param: workloadFilePath = "/home/tao/software/station/Workplace/data_template/workload.txt"
     * @author: tao
     * @date: 上午9:29 18-5-28
     */
    @Test
    public void testPrestoEvaluator() {
        String testEvalFuc = "orc"; // pixels, orc
        String testEvalCsv = testEvalFuc + "_duration.csv";
        String tableName = "test30g_" + testEvalFuc;

        String workloadFilePath = "/home/tao/software/data/pixels/test30G_pixels/lite_1000c_workload.txt";
        String logDir = "/home/tao/software/data/pixels/test30G_pixels/1000/";
//        String workloadFilePath = "/home/tao/software/data/pixels/test30G_pixels/105/lite_105c_workload.txt";
//        String logDir = "/home/tao/software/data/pixels/test30G_pixels/105/";

        ConfigFactory instance = ConfigFactory.Instance();
        Properties properties = new Properties();
//        String user = instance.getProperty("presto.user");
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
                properties.setProperty("user", user + "_" + lines[0]);
//                columns = lines[2];
                orderByColumn = getOrderByCol(lines[2]);
                long cost = PrestoEvaluator.execute(jdbc, properties, tableName, lines[2], orderByColumn);
                timeWriter.write(line.split("\t")[0] + "," + i + "," + cost + "\n");
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

    /**
     * @ClassName: TestEvaluator
     * @Title: getOrderByCol
     * @Description:
     * @param:
     * @author: tao
     * @date: 上午9:58 18-5-28
     */
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
