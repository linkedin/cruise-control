/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import java.util.Map;
import java.util.function.Supplier;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;

/**
 * Every {@link RawMetricType} corresponds to a Prometheus query that can be used to fetch the value for
 * that metric. This interface should be implemented to provide the mapping from {@link RawMetricType} to
 * Prometheus query string.
 *
 * The prometheus query will depend on the metric names stored in Prometheus, which in turn depend on how
 * the Prometheus JMX and Node exporters on the Kafka node are configured to apply transformations on metric names.
 * In case they do not have any special configuration, the {@link DefaultPrometheusQuerySupplier} can be used.
 */
public interface PrometheusQuerySupplier extends Supplier<Map<RawMetricType, String>> {
}
