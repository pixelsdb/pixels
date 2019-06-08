package cn.edu.ruc.iir.pixels.hive;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveClientTest {

    @Test
    public void SelectTest() {
        String hostUrl = "jdbc:hive2://presto00:10000/default";
        String userName = "iir";
        String passWord = "";
        HiveClient client = HiveClient.Instance(hostUrl, userName, passWord);

        String statement = "select Count(*) from pixelssd";
        ResultSet res = client.select(statement);
        try {
            while (res.next()) {
                System.out.println(res.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void IsTableExistTest() {
        HiveClient client = HiveClient.Instance("jdbc:hive2://presto00:10000/default", "presto", "");
        int count = client.IsTableExist("select count(*) from text");
        System.out.println(count);
    }

}
