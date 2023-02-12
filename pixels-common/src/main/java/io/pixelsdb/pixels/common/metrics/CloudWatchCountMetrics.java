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
package io.pixelsdb.pixels.common.metrics;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * @author hank
 * @date 1/2/23
 */
public class CloudWatchCountMetrics
{
    private static final String MetricsNamespace;
    private static final Dimension MetricsDimension;

    static
    {
        MetricsNamespace = ConfigFactory.Instance().getProperty("cloud.watch.metrics.namespace");
        String dimensionName = ConfigFactory.Instance().getProperty("cloud.watch.metrics.dimension.name");
        String dimensionValue = ConfigFactory.Instance().getProperty("cloud.watch.metrics.dimension.value");
        MetricsDimension = Dimension.builder().name(dimensionName).value(dimensionValue).build();
    }

    private final CloudWatchClient client = CloudWatchClient.builder().build();

    public void putCount(NamedCount count)
    {
        /**
         * Issue #385:
         * AWS CloudWatch only supports timestamps with seconds precision, but some JDKs may provide higher precision by default.
         */
        String time = ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS).format(DateTimeFormatter.ISO_INSTANT);
        Instant instant = Instant.parse(time);
        MetricDatum datum = MetricDatum.builder().metricName(count.getName()).unit(StandardUnit.COUNT)
                .value((double) count.getCount()).timestamp(instant).dimensions(MetricsDimension).storageResolution(1).build();
        PutMetricDataRequest request = PutMetricDataRequest.builder().namespace(MetricsNamespace)
                .metricData(datum).build();
        this.client.putMetricData(request);
    }
}
