package cn.edu.ruc.iir.rainbow.layout.sql;

import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.InputFactory;
import cn.edu.ruc.iir.rainbow.common.util.OutputFactory;
import cn.edu.ruc.iir.rainbow.layout.builder.ColumnOrderBuilder;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateQuery
{
	private GenerateQuery() {}

	private static List<String> genMergedJobs(String schemaFilePath, String workloadFilePath) throws IOException, ColumnNotFoundException
	{
		List<String> mergedJobs = new ArrayList<>();
		try (BufferedReader originWorkloadReader = InputFactory.Instance().getReader(workloadFilePath))
		{
			List<Query> workload = new ArrayList<>();
			Map<String, Column> columnMap = new HashMap<String, Column>();
			List<Column> columnOrder = ColumnOrderBuilder.build(new File(schemaFilePath));
			Map<Integer, String> columnIdToNameMap = new HashMap<>();
			for (Column column : columnOrder)
			{
				columnIdToNameMap.put(column.getId(), column.getName());
				columnMap.put(column.getName(), column);
			}

			String line = null;
			int qid = 0;
			while ((line = originWorkloadReader.readLine()) != null)
			{
				String[] tokens = line.split("\t");
				String[] columnNames = tokens[2].split(",");
				Query query = new Query(qid, tokens[0], Double.parseDouble(tokens[1]));
				for (String columnName : columnNames)
				{
					Column column = columnMap.get(columnName);
					if (column == null)
					{
						throw new ColumnNotFoundException("column " + columnName + " from query " +
								query.getId() + " is not found in the columnOrder.");
					}
					query.addColumnId(column.getId());
					column.addQueryId(qid);
				}

				boolean patternExists = false;
				for (Query query1 : workload)
				{
					boolean patternEquals = true;
					for (int columnId : query.getColumnIds())
					{
						if (!query1.getColumnIds().contains(columnId))
						{
							patternEquals = false;
							break;
						}
					}
					if (patternEquals && query.getColumnIds().size() == query1.getColumnIds().size())
					{
						query1.addWeight(query.getWeight());
						patternExists = true;
						break;
					}
				}
				if (patternExists == false)
				{
					workload.add(query);
					qid++;
				}
			}
			for (Query query : workload)
			{
				StringBuilder builder = new StringBuilder(query.getSid() + "\t");
				List<Integer> columnIds = new ArrayList<>(query.getColumnIds());
				String columnName = columnIdToNameMap.get(columnIds.get(0));
				builder.append(columnName);
				for (int i = 1; i < columnIds.size(); ++i)
				{
					builder.append("," + columnIdToNameMap.get(columnIds.get(i)));
				}
				mergedJobs.add(builder.toString());
			}
		}

		return mergedJobs;
	}

	private static Map<String, Double> getColumnToSizeMap (String schemaFilePath) throws IOException
	{
		Map<String, Double> columnToSizeMap = new HashMap<>();
		try (BufferedReader reader = InputFactory.Instance().getReader(schemaFilePath))
		{
			String line;
			while ((line = reader.readLine()) != null)
			{
				String[] splits = line.split("\t");
				columnToSizeMap.put(splits[0], Double.parseDouble(splits[2]));
			}
		}
		return columnToSizeMap;
	}

    private static final String dataDir = ConfigFactory.Instance().getProperty("data.dir");

    /**
     * Currently only Parquet data format is supported in generated Spark queries.
     * @param tableName
     * @param namenode in form of localhost:9000
     * @param schemaFilePath
     * @param workloadFilePath
     * @param sparkQueryPath
     * @param hiveQueryPath
     * @throws IOException
     * @throws ColumnNotFoundException
     */
	public static void Gen(String tableName, String namenode, String schemaFilePath, String workloadFilePath,
    String sparkQueryPath, String hiveQueryPath) throws IOException, ColumnNotFoundException
	{
		List<String> mergedJobs = genMergedJobs(schemaFilePath, workloadFilePath);
		Map<String, Double> columnToSizeMap = getColumnToSizeMap(schemaFilePath);

		try (BufferedWriter sparkWriter = OutputFactory.Instance().getWriter(sparkQueryPath);
             BufferedWriter hiveWriter = OutputFactory.Instance().getWriter(hiveQueryPath))
		{
			int i = 0;

			for (String line : mergedJobs)
			{
				String tokens[] = line.split("\t");
				String columns = tokens[1];

				// get the smallest column as the order by column
				String orderByColumn = null;
				double size = Double.MAX_VALUE;

				for (String name : columns.split(","))
				{
					if (name.equalsIgnoreCase("market"))
					{
						orderByColumn = name;
						break;
					}
				}

				if (orderByColumn == null)
				{
					for (String name : columns.split(","))
					{
						if (columnToSizeMap.get(name) < size)
						{
							size = columnToSizeMap.get(name);
							orderByColumn = name;
						}
					}
				}
				
				sparkWriter.write("% query " + i + "\n");
				sparkWriter.write("val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n");
				sparkWriter.write("import sqlContext.createSchemaRDD\n");
				sparkWriter.write("val parquetFile = sqlContext.parquetFile(\"hdfs://" + namenode + dataDir + "/" + tableName + "\")\n");
				sparkWriter.write("parquetFile.registerTempTable(\"parq\")\n");
				sparkWriter.write("val res = sqlContext.sql(\"SELECT " + columns + " FROM parq ORDER BY " + orderByColumn + " LIMIT 3000\")\n");
				sparkWriter.write("res.count()\n");
				sparkWriter.newLine();
				sparkWriter.newLine();
				hiveWriter.write("% query " + i + "\n");
				hiveWriter.write("SELECT " + columns + " FROM " + tableName + " ORDER BY " + orderByColumn + " LIMIT 3000;\n");
				hiveWriter.newLine();
				hiveWriter.newLine();
				i++;
			}
		}
	}
}
