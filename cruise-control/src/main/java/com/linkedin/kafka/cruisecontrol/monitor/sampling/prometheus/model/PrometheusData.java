/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model;

import java.util.List;
import java.util.Objects;
import com.google.gson.annotations.SerializedName;

/**
 * Encapsulates the results of the Prometheus API call.
 */
public class PrometheusData {
    @SerializedName("result")
    private final List<PrometheusQueryResult> _result;

    public PrometheusData(List<PrometheusQueryResult> result) {
        _result = result;
    }

    /**
     * @return List of query results.
     */
    public List<PrometheusQueryResult> result() {
        return _result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrometheusData that = (PrometheusData) o;
        return Objects.equals(_result, that._result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_result);
    }
}
