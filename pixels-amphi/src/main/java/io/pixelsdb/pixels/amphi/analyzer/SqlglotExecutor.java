/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.amphi.analyzer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.pixelsdb.pixels.common.exception.AmphiException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

/**
 * Executor to call external SQLglot utility process.
 */
public class SqlglotExecutor
{
    /**
     * Transpile a SQL statement from one dialect to another.
     * @param sqlStatement
     * @param fromDialect
     * @param toDialect
     * @return transpiled SQL statement
     * @throws IOException
     * @throws InterruptedException
     * @throws AmphiException
     */
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

    /**
     * Parse the scanned columns in SQL statement.
     * @param sqlStatement
     * @return the list of column scanned
     * @throws IOException
     * @throws InterruptedException
     * @throws AmphiException
     */
    public List<String> parseColumnFields(String sqlStatement)
            throws IOException, InterruptedException, AmphiException
    {
        InputStream scriptInputStream = SqlglotExecutor.class.getResourceAsStream("/scripts/sqlglot_metadata.py");
        Path scriptPath = Files.createTempFile("sqlglot_metadata", ".py");

        Files.copy(scriptInputStream, scriptPath, StandardCopyOption.REPLACE_EXISTING);

        Process sqlglotProcess = new ProcessBuilder("python3", scriptPath.toString(), sqlStatement, "column")
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

        List<String> columnList = JSON.parseObject(output.toString(), new TypeReference<List<String>>(){});

        return columnList;
    }

    private static String getProcessOutput(InputStream inputStream) throws IOException
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = reader.readLine();

        return line;
    }
}

