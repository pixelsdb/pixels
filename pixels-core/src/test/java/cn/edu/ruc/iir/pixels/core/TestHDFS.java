package cn.edu.ruc.iir.pixels.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import javax.security.sasl.AuthenticationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

/**
 * pixels
 *
 * @author guodong
 */
public class TestHDFS
{
    @Test
    public void testGetFileByBlockId() throws IOException
    {
        Configuration conf = new Configuration();
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        URLConnectionFactory connectionFactory =
                URLConnectionFactory.newDefaultURLConnectionFactory(conf);
        long blockId = 1073742421;
        long start = System.currentTimeMillis();
        String file = "";
        for (int i = 0; i < 10000; i++) {
            file = getFileByBlockId(ugi, connectionFactory, blockId);
        }
        long end = System.currentTimeMillis();
        System.out.println("Cost: " + (end - start));
        System.out.println(file);
    }

    private String getFileByBlockId(UserGroupInformation ugi, URLConnectionFactory connectionFactory,
                                  long blockId) throws IOException
    {
        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append("/fsck?ugi=").append(ugi.getShortUserName());
        urlBuilder.append("&blockId=").append("blk_").append(blockId);
        urlBuilder.insert(0, "http://localhost:50070");
        urlBuilder.append("&path=").append("%2F");
        URL path = new URL(urlBuilder.toString());
        URLConnection connection;
        try {
            connection = connectionFactory.openConnection(path);
        }
        catch (AuthenticationException e) {
            throw new IOException(e);
        }
        InputStream stream = connection.getInputStream();
        String line = null;
        String file = "";
        try (BufferedReader input = new BufferedReader(
                new InputStreamReader(stream, "UTF-8"))) {
            while ((line = input.readLine()) != null) {
                if (line.startsWith("Block belongs to:")) {
                    file = line.split(": ")[1].trim();
                }
            }
        }
        stream.close();
        return file;
    }
}
