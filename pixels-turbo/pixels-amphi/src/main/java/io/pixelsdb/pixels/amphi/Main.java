package io.pixelsdb.pixels.amphi;

import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.parser.PixelsParser;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

import java.util.Properties;

public class Main
{
    public static void main(String[] args) throws InterruptedException, SqlParseException
    {
        String hostAddr = "ec2-18-218-128-203.us-east-2.compute.amazonaws.com";

        MetadataService instance = new MetadataService(hostAddr, 18888);

        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL_ANSI)
                .setParserFactory(SqlParserImpl.FACTORY)
                .build();

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

        PixelsParser tpchPixelsParser = new PixelsParser(instance, "tpch", parserConfig, properties);

        String query = "SELECT * FROM CUSTOMER";
        SqlNode parsedNode = tpchPixelsParser.parseQuery(query);
        System.out.println("Parsed SQL Query: \n" + parsedNode);

        SqlNode validatedNode = tpchPixelsParser.validate(parsedNode);
        System.out.println("No exception, validation success.");

        RelNode rel = tpchPixelsParser.toRelNode(validatedNode);
        final RelJsonWriter writer = new RelJsonWriter();
        rel.explain(writer);
        System.out.println("Logical plan: \n" + writer.asString());

        instance.shutdown();

        System.out.println("Hello world!");
    }
}