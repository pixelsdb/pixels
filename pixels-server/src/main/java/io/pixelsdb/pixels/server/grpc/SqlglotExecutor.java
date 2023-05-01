package io.pixelsdb.pixels.server.grpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class SqlglotExecutor
{
    public String transpileSql(String sqlStatement, String fromDialect, String toDialect)
            throws IOException, InterruptedException
    {
        InputStream scriptInputStream = SqlglotExecutor.class.getResourceAsStream("/scripts/sqlglot_transpile.py");
        Path scriptPath = Files.createTempFile("sqlglot_transpile", ".py");

        Files.copy(scriptInputStream, scriptPath, StandardCopyOption.REPLACE_EXISTING);

        Process pythonProcess = new ProcessBuilder("python3", scriptPath.toString(),
                sqlStatement, fromDialect, toDialect)
                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                .start();
        pythonProcess.waitFor();

        String output = getProcessOutput(pythonProcess.getInputStream());

        return output;
    }

    private static String getProcessOutput(InputStream inputStream) throws IOException
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = reader.readLine();

        return line;
    }
}

