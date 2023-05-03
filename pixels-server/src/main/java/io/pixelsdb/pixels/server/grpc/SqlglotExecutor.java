package io.pixelsdb.pixels.server.grpc;

import io.pixelsdb.pixels.common.exception.AmphiException;

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
            throws IOException, InterruptedException, AmphiException
    {
        InputStream scriptInputStream = SqlglotExecutor.class.getResourceAsStream("/scripts/sqlglot_transpile.py");
        Path scriptPath = Files.createTempFile("sqlglot_transpile", ".py");

        Files.copy(scriptInputStream, scriptPath, StandardCopyOption.REPLACE_EXISTING);

        Process sqlglotProcess = new ProcessBuilder("python3", scriptPath.toString(),
                sqlStatement, fromDialect, toDialect)
                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                .start();

        int exitCode = sqlglotProcess.waitFor();
        String output = getProcessOutput(sqlglotProcess.getInputStream());
        String errorMsg = getProcessOutput(sqlglotProcess.getErrorStream());

        switch (exitCode)
        {
            case 0:
                break;
            case 1:
                throw new AmphiException(errorMsg);
            default:
                throw new RuntimeException("Unknown error occurred with exit code " + exitCode + ": " + errorMsg);
        }

        return output;
    }

    private static String getProcessOutput(InputStream inputStream) throws IOException
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = reader.readLine();

        return line;
    }
}

