/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model;

import java.util.List;
import java.util.Objects;

import com.google.gson.annotations.SerializedName;

public class PrometheusQueryResult {
    @SerializedName("metric")
    final private PrometheusMetric _metric;
    @SerializedName("values")
    final private List<PrometheusValue> _values;

    public PrometheusQueryResult(PrometheusMetric metric, List<PrometheusValue> values) {
        _metric = metric;
        _values = values;
    }

    public PrometheusMetric metric() {
        return _metric;
    }

    public List<PrometheusValue> values() {
        return _values;
    }

    @Override
    public String toString() {
        return "PrometheusQueryResult{" +
            "_metric=" + _metric +
            ", _values=" + _values +
            '}';
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
        return Objects.equals(_metric, result._metric) &&
            Objects.equals(_values, result._values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_metric, _values);
    }
}
