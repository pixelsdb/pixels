package io.pixelsdb.pixels.common.metrics;

import org.junit.Test;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created at: 29/12/2022
 * Author: hank
 */
public class CloudWatchTest
{
    @Test
    public void test()
    {
        Dimension dimension = Dimension.builder().name("pixels_cluster").value("02").build();
        String time = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
        Instant instant = Instant.parse(time);
        MetricDatum datum = MetricDatum.builder().metricName("vm_queue_size").unit(StandardUnit.COUNT)
                .value(1000.0).timestamp(instant).dimensions(dimension).storageResolution(1).build();

        PutMetricDataRequest request = PutMetricDataRequest.builder().namespace("Auto Scaling/Pixels")
                .metricData(datum).build();
        CloudWatchClient cloudWatchClient = CloudWatchClient.builder().build();
        cloudWatchClient.putMetricData(request);
    }
}
