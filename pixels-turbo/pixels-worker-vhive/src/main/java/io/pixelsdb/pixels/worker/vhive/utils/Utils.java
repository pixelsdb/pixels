package io.pixelsdb.pixels.worker.vhive.utils;

import one.profiler.AsyncProfiler;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Utils {
    private static final AsyncProfiler PROFILER = AsyncProfiler.getInstance();
    private static final String EVENT = System.getenv("PROFILING_EVENT");
    private static final String FTP_HOST = System.getenv("FTP_HOST");
    private static final String FTP_PORT = System.getenv("FTP_PORT");
    private static final String FTP_USERNAME = System.getenv("FTP_USERNAME");
    private static final String FTP_PASSWORD = System.getenv("FTP_PASSWORD");

    public static void append(String src, String dest) throws IOException {
        // append the log file to FTP server
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(FTP_HOST, Integer.parseInt(FTP_PORT));
        ftpClient.login(FTP_USERNAME, FTP_PASSWORD);
        ftpClient.enterLocalPassiveMode();
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

        FileInputStream inputStream = new FileInputStream(src);
        ftpClient.appendFile(dest, inputStream);
        inputStream.close();
        ftpClient.logout();
    }

    public static void upload(String src, String dest) throws IOException {
        // store the JFR profiling file to FTP server
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(FTP_HOST, Integer.parseInt(FTP_PORT));
        ftpClient.login(FTP_USERNAME, FTP_PASSWORD);
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
