package io.pixelsdb.pixels.parser;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestPixelsParser
{
    String hostAddr = "ec2-18-218-128-203.us-east-2.compute.amazonaws.com";

    MetadataService instance = null;

    @Before
    public void init()
    {
        this.instance = new MetadataService(hostAddr, 18888);
    }

    @After
    public void shutdown() throws InterruptedException
    {
        this.instance.shutdown();
    }

    @Test
    public void testCalciteFunc() throws Exception
    {
        String query = TpchQuery.Q8;

        // Build Parser
        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL_ANSI)
                .setParserFactory(SqlParserImpl.FACTORY)
                .build();
        SqlParser parser = SqlParser.create(query, parserConfig);

        SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        RexBuilder rexBuilder = new RexBuilder(typeFactory);

        SqlNode sqlNode = parser.parseStmt();

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        PixelsSchema pixelsSchema = new PixelsSchema("tpch", this.instance);
        rootSchema.add("tpch", pixelsSchema);

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(rootSchema,
                rootSchema.path("tpch"),
                typeFactory,
                new CalciteConnectionConfigImpl(properties));

        SqlValidatorWithHints validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory);

        SqlNode validatedSqlNode = validator.validate(sqlNode);

        // Build Volcano Planner
        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.EMPTY_CONTEXT);
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.registerAbstractRelationalRules();
        RelOptUtil.registerAbstractRels(planner);


        // Add Transformation Rules
        for (RelOptRule rule : TRANSFORM_RULES) {
            planner.addRule(rule);
        }

        // Add Implementation Rules (Enumerable)
        for (RelOptRule rule : ENUMERABLE_RULES) {
            planner.addRule(rule);
        }

        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        // Build SqlToRelConverter
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withInSubQueryThreshold(Integer.MAX_VALUE)
                .withExpand(false);
        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(null, validator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE, converterConfig);

        // Convert SqlNode to RelNode
        RelRoot relRoot = sqlToRelConverter.convertQuery(validatedSqlNode, false, true);

        RelNode rel = relRoot.rel;
        final RelJsonWriter writer = new RelJsonWriter();
        rel.explain(writer);
        System.out.println(writer.asString());

        System.out.println("OK");
    }

    private static final List<RelOptRule> TRANSFORM_RULES =
            ImmutableList.of(
                    CoreRules.AGGREGATE_STAR_TABLE,
                    CoreRules.AGGREGATE_PROJECT_STAR_TABLE,
                    CalciteSystemProperty.COMMUTE.value()
                            ? CoreRules.JOIN_ASSOCIATE
                            : CoreRules.PROJECT_MERGE,
                    CoreRules.FILTER_SCAN,
                    CoreRules.PROJECT_FILTER_TRANSPOSE,
                    CoreRules.FILTER_PROJECT_TRANSPOSE,
                    CoreRules.FILTER_INTO_JOIN,
                    CoreRules.JOIN_PUSH_EXPRESSIONS,
                    CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
                    CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT,
                    CoreRules.AGGREGATE_CASE_TO_FILTER,
                    CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
                    CoreRules.FILTER_AGGREGATE_TRANSPOSE,
                    CoreRules.PROJECT_WINDOW_TRANSPOSE,
                    CoreRules.MATCH,
                    CoreRules.JOIN_COMMUTE,
                    JoinPushThroughJoinRule.RIGHT,
                    JoinPushThroughJoinRule.LEFT,
                    CoreRules.SORT_PROJECT_TRANSPOSE,
                    CoreRules.SORT_JOIN_TRANSPOSE,
                    CoreRules.SORT_REMOVE_CONSTANT_KEYS,
                    CoreRules.SORT_UNION_TRANSPOSE,
                    CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS,
                    CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS);

    public static final List<RelOptRule> ENUMERABLE_RULES =
            ImmutableList.of(
                    EnumerableRules.ENUMERABLE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_CORRELATE_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_RULE,
                    EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                    EnumerableRules.ENUMERABLE_SORT_RULE,
                    EnumerableRules.ENUMERABLE_LIMIT_RULE,
                    EnumerableRules.ENUMERABLE_COLLECT_RULE,
                    EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
                    EnumerableRules.ENUMERABLE_UNION_RULE,
                    EnumerableRules.ENUMERABLE_INTERSECT_RULE,
                    EnumerableRules.ENUMERABLE_MINUS_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
                    EnumerableRules.ENUMERABLE_VALUES_RULE,
                    EnumerableRules.ENUMERABLE_WINDOW_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE);

}
