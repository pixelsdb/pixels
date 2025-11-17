package io.pixelsdb.pixels.server;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.server.ExecutionHint;
import io.pixelsdb.pixels.common.server.QueryStatus;
import io.pixelsdb.pixels.common.server.rest.request.GetQueryResultRequest;
import io.pixelsdb.pixels.common.server.rest.request.GetQueryStatusRequest;
import io.pixelsdb.pixels.common.server.rest.request.SubmitQueryRequest;
import io.pixelsdb.pixels.common.server.rest.response.GetQueryResultResponse;
import io.pixelsdb.pixels.common.server.rest.response.GetQueryStatusResponse;
import io.pixelsdb.pixels.common.server.rest.response.SubmitQueryResponse;
import io.pixelsdb.pixels.server.constant.RestUrlPath;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class QueryExecutionInfo {
    int queryID;
    String tranceToken;
    ExecutionHint executionHint;
    long submissionTime;
    double pendingTime;
    double executionTime;
    double costCents;
    double billedCents;
    boolean isFinished;
    //String resultJson;

    public QueryExecutionInfo(int queryID, String tranceToken, ExecutionHint executionHint, long submissionTime, boolean isFinished) {
        this.queryID = queryID;
        this.tranceToken = tranceToken;
        this.executionHint = executionHint;
        this.submissionTime = submissionTime;
        this.isFinished = isFinished;
    }

    public String getTraceToken() { return this.tranceToken; }

    public String getExecutionHint() { return "" + this.executionHint; }

    public void setPendingTime(double pendingTime) { this.pendingTime = pendingTime; }

    public void setExecutionTime(double executionTime) { this.executionTime = executionTime; }

    public void setCostCents(double costCents) { this.costCents = costCents; }

    public void setBilledCents(double billedCents) { this.billedCents = billedCents; }

    public boolean isFinished() { return this.isFinished; }

    public void setFinished() { this.isFinished = true; }

    //public String getResultJson() { return this.resultJson; }

    //public void setResultJson(String resultJson) { this.resultJson = resultJson; }

    @Override
    public String toString() {
        // 将queryID从数字转换为对应的查询标识符
        String queryIdentifier = "Q" + queryID;

        // 格式化submissionTime为日期字符串
        String submissionTimeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(submissionTime));

        return "QueryExecutionInfo{" +
                "queryID=" + queryIdentifier +
                ", executionHint=" + executionHint +
                ", submissionTime=" + submissionTimeStr +
                ", pendingTime=" + pendingTime + " ms" +
                ", executionTime=" + executionTime + " ms" +
                ", costCents=" + String.format("%.10f", costCents) +
                ", billedCents=" + billedCents +
		//", resultJson=" + resultJson +
                '}';
    }
}

@SpringBootTest
@AutoConfigureMockMvc
public class Experiment {
    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testExecutionTime() throws  Exception {
        // 创建一个线程安全的List来存储每个查询的执行信息
        List<QueryExecutionInfo> queryExecutionInfos = Collections.synchronizedList(new ArrayList<>());

        List<String> lines = Files.readAllLines(Paths.get("/home/ubuntu/pixels/pixels-server/src/test/java/io/pixelsdb/pixels/server/queries.txt"));
        long startTime = System.currentTimeMillis();

        long num = 1;
        for (String line : lines) {
            String[] parts = line.split(",", 4);
            if (parts.length < 3) {
                continue;
            }

            long start = Long.parseLong(parts[0].trim());
            String execution_hint = parts[1].trim();
            ExecutionHint executionHint = null;
            switch (execution_hint) {
                case "relaxed":
                    executionHint = ExecutionHint.RELAXED;
                    break;
                case "best_of_effort":
                    executionHint = ExecutionHint.BEST_OF_EFFORT;
                    break;
                case "immediately":
                    executionHint = ExecutionHint.IMMEDIATE;
                    break;
            }
            executionHint = ExecutionHint.IMMEDIATE;
            int query_id = Integer.parseInt(parts[2].trim());
            String query = parts[3].trim();

            // wait until the start time
            long currentTime = System.currentTimeMillis();
            long delay = start - (currentTime - startTime);
            if (delay > 0) {
                // System.out.println(delay + " delay > 0");
                Thread.sleep(delay);
            }

            long submissionTime = System.currentTimeMillis();

            // Create and submit the query
            String json = this.mockMvc.perform(
                            post(RestUrlPath.SUBMIT_QUERY).contentType(MediaType.APPLICATION_JSON).content(
                                    JSON.toJSONString(new SubmitQueryRequest(
                                            query,
                                            executionHint, 10))))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
            SubmitQueryResponse response = JSON.parseObject(json, SubmitQueryResponse.class);

            queryExecutionInfos.add(new QueryExecutionInfo(query_id, response.getTraceToken(), executionHint, submissionTime, false));
            System.out.println(num + " execute " + response.getTraceToken());
            num += 1;
        }

        num = 1;
        boolean isAllFinished;
        do
        {
            isAllFinished = true;
            for(QueryExecutionInfo info: queryExecutionInfos) {
                if(info.isFinished()) {
                    continue;
                }
                // System.out.println(info.getExecutionHint());
                String statusJson = this.mockMvc.perform(post(RestUrlPath.GET_QUERY_STATUS)
                                .contentType(MediaType.APPLICATION_JSON).content(
                                        JSON.toJSONString(new GetQueryStatusRequest(Arrays.asList(info.getTraceToken())))))
                        .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
                GetQueryStatusResponse queryStatus = JSON.parseObject(statusJson, GetQueryStatusResponse.class);
                if (queryStatus.getQueryStatuses().get(info.getTraceToken()) == QueryStatus.FINISHED) {
                    info.setFinished();
                    String resultJson = this.mockMvc.perform(post(RestUrlPath.GET_QUERY_RESULT)
                                    .contentType(MediaType.APPLICATION_JSON).content(
                                            JSON.toJSONString(new GetQueryResultRequest(info.getTraceToken()))))
                            .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
                    GetQueryResultResponse result = JSON.parseObject(resultJson, GetQueryResultResponse.class);
                    // System.out.println(resultJson);
                    info.setPendingTime(result.getPendingTimeMs());
                    info.setExecutionTime(result.getExecutionTimeMs());
                    info.setCostCents(result.getCostCents());
                    info.setBilledCents(result.getBilledCents());
		    //info.setResultJson(resultJson);
                    System.out.println(num + ' ' + info.getTraceToken() + " is finished");
		    num += 1;
                } else {
                    isAllFinished = false;
                }
            }
            TimeUnit.SECONDS.sleep(1);
        } while (!isAllFinished);
        System.out.println("all finished");

        // 定义文件路径
        String filePath = "/home/ubuntu/pixels/pixels-server/query_execution_info.txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (QueryExecutionInfo info : queryExecutionInfos) {
                // 将每个QueryExecutionInfo对象的toString()结果写入文件
                writer.write(info.toString());
                writer.newLine(); // 添加换行符，以便每个对象的信息都在新的一行开始
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
