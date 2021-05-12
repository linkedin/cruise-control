/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model;

import java.util.List;
import java.util.Objects;
import com.google.gson.annotations.SerializedName;

/**
 * Encapsulates the query result obtained from Prometheus API corresponding
 * to a single metric that matches the query made in the API call.
 * Multiple such results can be returned as part of a query_range API call
 * if multiple metrics match the query that was made.
 */
public class PrometheusQueryResult {
    @SerializedName("metric")
    private final PrometheusMetric _metric;
    @SerializedName("values")
    private final List<PrometheusValue> _values;

    public PrometheusQueryResult(PrometheusMetric metric, List<PrometheusValue> values) {
        _metric = metric;
        _values = values;
    }

    /**
     * @return Encapsulates the details about the metric that was matched to the query.
     */
    public PrometheusMetric metric() {
        return _metric;
    }

    /**
     * @return List of values for the metric, with their respective timestamps.
     */
    public List<PrometheusValue> values() {
        return _values;
    }

    @Override
    public String toString() {
        return "PrometheusQueryResult{_metric=" + _metric + ", _values=" + _values + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrometheusQueryResult result = (PrometheusQueryResult) o;
        return Objects.equals(_metric, result._metric) && Objects.equals(_values, result._values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_metric, _values);
    }
}
