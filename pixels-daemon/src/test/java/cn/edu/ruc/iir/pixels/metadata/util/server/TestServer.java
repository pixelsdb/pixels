package cn.edu.ruc.iir.pixels.metadata.util.server;

import cn.edu.ruc.iir.pixels.metadata.server.TimeServer;
import org.junit.Test;

import java.sql.SQLException;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.metadata.util.server
 * @ClassName: TestServer
 * @Description:
 * @author: tao
 * @date: Create in 2018-01-27 10:46
 **/
public class TestServer {

    public static void main(String[] args) {
        TimeServer server = new TimeServer();
        try {
            server.bind(18888);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
