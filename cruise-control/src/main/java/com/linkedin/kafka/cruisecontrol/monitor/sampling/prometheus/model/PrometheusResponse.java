/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model;

import java.util.Objects;
import com.google.gson.annotations.SerializedName;

/**
 * Represents the response sent from the Prometheus HTTP API.
 */
public class PrometheusResponse {
    @SerializedName("status")
    private final String _status;
    @SerializedName("data")
    private final PrometheusData _data;

    public PrometheusResponse(String status, PrometheusData data) {
        _status = status;
        _data = data;
    }

    /**
     * @return Status of the API call. Expected to be "success" if call was successful.
     */
    public String status() {
        return _status;
    }

    /**
     *
     * @return Data encapsulating the results from the API call.
     */
    public PrometheusData data() {
        return _data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrometheusResponse response = (PrometheusResponse) o;
        return Objects.equals(_status, response._status) && Objects.equals(_data, response._data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_status, _data);
    }
}
