package io.pixelsdb.pixels.worker.vhive.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.serializer.SerializerFeature;
import one.profiler.AsyncProfiler;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Utils {
    private static final AsyncProfiler PROFILER = AsyncProfiler.getInstance();
    private static final String EVENT = System.getenv("PROFILING_EVENT");
    private static final String FTP_HOST = System.getenv("FTP_HOST");
    private static final String FTP_PORT = System.getenv("FTP_PORT");
    private static final String FTP_USERNAME = System.getenv("FTP_USERNAME");
    private static final String FTP_PASSWORD = System.getenv("FTP_PASSWORD");
    private static final String FTP_WORKDIR = System.getenv("FTP_WORKDIR");

    private static void createDirectoryTree(FTPClient client, String dirTree) throws IOException {
        if (dirTree.startsWith(client.printWorkingDirectory())) {
            dirTree = dirTree.substring(client.printWorkingDirectory().length());
        }
        //tokenize the string and attempt to change into each directory level.  If you cannot, then start creating.
        String[] directories = dirTree.split("/");
        for (String dir : directories) {
            if (!dir.isEmpty()) {
                if (!client.changeWorkingDirectory(dir)) {
                    if (!client.makeDirectory(dir)) {
                        throw new IOException("Unable to create remote directory '" + dir + "'.  error='" + client.getReplyString() + "'");
                    }
                    if (!client.changeWorkingDirectory(dir)) {
                        throw new IOException("Unable to change into newly created remote directory '" + dir + "'.  error='" + client.getReplyString() + "'");
                    }
                }
            }
        }
    }

    public static void append(String src, String dest) throws IOException {
        // append the log file to FTP server
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(FTP_HOST, Integer.parseInt(FTP_PORT));
        ftpClient.login(FTP_USERNAME, FTP_PASSWORD);
        ftpClient.enterLocalPassiveMode();
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

        dest = String.format("%s/%s", FTP_WORKDIR, dest);
        String dir = dest.substring(0, dest.lastIndexOf("/"));
        createDirectoryTree(ftpClient, dir);

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

        dest = String.format("%s/%s", FTP_WORKDIR, dest);
        String dir = dest.substring(0, dest.lastIndexOf("/"));
        createDirectoryTree(ftpClient, dir);

        FileInputStream inputStream = new FileInputStream(src);
        ftpClient.storeFile(dest, inputStream);
        inputStream.close();
        ftpClient.logout();
    }

    public static void dump(String filename, Object ...contents) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(filename);
        JSONArray jsonArray = new JSONArray();
        jsonArray.addAll(Arrays.asList(contents));
        String output = jsonArray.toString(SerializerFeature.PrettyFormat, SerializerFeature.DisableCircularReferenceDetect);
        outputStream.write(output.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
    }

    public static void startProfile(String filename) throws IOException {
        PROFILER.execute(String.format("start,jfr,threads,event=%s,file=%s", EVENT, filename));
    }

    public static void stopProfile(String filename) throws IOException {
        PROFILER.execute(String.format("stop,file=%s", filename));
    }
}
