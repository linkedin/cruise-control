/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model;

import java.util.Objects;

import com.google.gson.annotations.SerializedName;

public class PrometheusResponse {
    @SerializedName("status")
    final private String _status;
    @SerializedName("data")
    final private PrometheusData _data;

    public PrometheusResponse(String status, PrometheusData data) {
        _status = status;
        _data = data;
    }

    public String status() {
        return _status;
    }

    public PrometheusData data() {
        return _data;
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrometheusResponse response = (PrometheusResponse) o;
        return Objects.equals(_status, response._status) &&
            Objects.equals(_data, response._data);
    }

    @Override public int hashCode() {
        return Objects.hash(_status, _data);
    }
}
