package io.pixelsdb.pixels.worker.vhive.utils;

import one.profiler.AsyncProfiler;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Utils {
    private static final AsyncProfiler profiler = AsyncProfiler.getInstance();
    public static void upload(String src, String dest) throws IOException {
        // store the JFR profiling file to FTP server
        String hostname = System.getenv("FTP_HOST");
        int port = Integer.parseInt(System.getenv("FTP_PORT"));
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(hostname, port);
        String username = System.getenv("FTP_USERNAME");
        String password = System.getenv("FTP_PASSWORD");
        ftpClient.login(username, password);
        ftpClient.enterLocalPassiveMode();
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

        FileInputStream inputStream = new FileInputStream(src);
        ftpClient.storeFile(dest, inputStream);
        inputStream.close();
        ftpClient.logout();
    }
    public static void dump(String filename, String content) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(filename);
        outputStream.write(content.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
    }

    public static void startProfile(String event, String filename) throws IOException {
        profiler.execute(String.format("start,jfr,threads,event=%s,file=%s", event, filename));
    }

    public static void stopProfile(String filename) throws IOException {
        profiler.execute(String.format("stop,file=%s", filename));
    }
}
