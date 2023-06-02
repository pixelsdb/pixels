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
package io.pixelsdb.pixels.parser;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.List;
import java.util.Properties;

/**
 * PixelsParser incorporates the workflow of Calcite parser, validator and logical planner.
 */
public class PixelsParser
{

    private final MetadataService metadataService;
    private final SqlParser.Config parserConfig;
    private final RexBuilder rexBuilder;
    private final CalciteSchema schema;
    private final String schemaName;
    private final CalciteCatalogReader catalogReader;
    private final SqlValidatorWithHints validator;
    private final VolcanoPlanner planner;

    public PixelsParser(MetadataService ms,
                        String schemaName,
                        SqlParser.Config parserConfig,
                        Properties calciteConfig)
    {
        this.metadataService = ms;
        this.parserConfig = parserConfig;
        SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        this.rexBuilder = new RexBuilder(typeFactory);

        this.schema = CalciteSchema.createRootSchema(true);
        this.schema.add(schemaName, new PixelsSchema(schemaName, metadataService));
        this.schemaName = schemaName;
        this.catalogReader = new CalciteCatalogReader(schema,
                schema.path(schemaName),
                typeFactory,
                new CalciteConnectionConfigImpl(calciteConfig));

        this.validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory);

        this.planner = createPlanner();
    }

    private VolcanoPlanner createPlanner()
    {
        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.EMPTY_CONTEXT);
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.registerAbstractRelationalRules();
        RelOptUtil.registerAbstractRels(planner);

        for (RelOptRule rule : TRANSFORM_RULES) {
            planner.addRule(rule);
        }

        for (RelOptRule rule : ENUMERABLE_RULES) {
            planner.addRule(rule);
        }

        return planner;
    }

    public SqlNode parseQuery(String sql) throws SqlParseException
    {
        SqlParser parser = SqlParser.create(sql, parserConfig);
        return parser.parseQuery(); // Assumed only analytical workload
    }

    public SqlNode validate(SqlNode sqlNode)
    {
        return validator.validate(sqlNode);
    }

    public RelNode toRelNode(SqlNode sqlNode)
    {
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withInSubQueryThreshold(Integer.MAX_VALUE)
                .withExpand(false);
        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(null, validator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE, converterConfig);

        RelRoot relRoot = sqlToRelConverter.convertQuery(sqlNode, false, true);

        return relRoot.rel;
    }

    public SqlParser.Config getParserConfig()
    {
        return parserConfig;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public CalciteSchema getSchema()
    {
        return schema;
    }

    public MetadataService getMetadataService()
    {
        return metadataService;
    }

    public SqlValidatorWithHints getValidator()
    {
        return validator;
    }

    public VolcanoPlanner getPlanner()
    {
        return planner;
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
