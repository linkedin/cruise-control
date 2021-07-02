/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model;

import java.util.Objects;
import javax.annotation.Nullable;
import com.google.gson.annotations.SerializedName;

/**
 * Represents the details of a Prometheus metric that matched the query
 * made to the Prometheus query_range API.
 */
public class PrometheusMetric {
    @SerializedName("instance")
    private final String _instance;
    @SerializedName("topic")
    final @Nullable private String _topic;
    @SerializedName("partition")
    final @Nullable private String _partition;

    public PrometheusMetric(String instance,
                            @Nullable String topic,
                            @Nullable String partition) {
        _instance = instance;
        _topic = topic;
        _partition = partition;
    }

    /**
     * @return The host and port from which this metric is reported.
     */
    public String instance() {
        return _instance;
    }

    /**
     * @return The topic for which this metric is reported. Will be
     * null if the metric is not a topic-level or partition-level metric.
     */
    @Nullable
    public String topic() {
        return _topic;
    }

    /**
     * @return The partition for which this metric is reported.
     * Will be null if the metric is not a partition-level metric.
     */
    @Nullable
    public String partition() {
        return _partition;
    }

    @Override
    public String toString() {
        return "PrometheusMetric{_instance='" + _instance + '\'' + ", _topic='" + _topic + '\'' + ", _partition='" + _partition + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrometheusMetric that = (PrometheusMetric) o;
        return Objects.equals(_instance, that._instance) && Objects.equals(_topic, that._topic) && Objects.equals(_partition, that._partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_instance, _topic, _partition);
    }
}
