/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import java.util.Map;
import java.util.function.Supplier;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;

/**
 * Query supplier.
 */
public interface PrometheusQuerySupplier extends Supplier<Map<RawMetricType, String>> {
}
