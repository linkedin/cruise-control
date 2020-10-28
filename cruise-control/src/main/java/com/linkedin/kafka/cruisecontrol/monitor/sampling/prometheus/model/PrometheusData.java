/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model;

import java.util.List;
import java.util.Objects;

import com.google.gson.annotations.SerializedName;

public class PrometheusData {
    @SerializedName("resultType")
    final private String _resultType;
    @SerializedName("result")
    final private List<PrometheusQueryResult> _result;

    public PrometheusData(String resultType, List<PrometheusQueryResult> result) {
        _resultType = resultType;
        _result = result;
    }

    public String resultType() {
        return _resultType;
    }

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
        return Objects.equals(_resultType, that._resultType) &&
            Objects.equals(_result, that._result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_resultType, _result);
    }
}
