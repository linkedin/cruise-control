/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model;

import java.util.Objects;
import javax.annotation.Nullable;

import com.google.gson.annotations.SerializedName;

public class PrometheusMetric {
    @SerializedName("__name__")
    final private String _domainName;
    @SerializedName("instance")
    final private String _instance;
    @SerializedName("job")
    final private String _job;
    @SerializedName("name")
    final private String _name;
    @SerializedName("topic")
    final @Nullable private String _topic;
    @SerializedName("partition")
    final @Nullable private String _partition;

    public PrometheusMetric(
        String domainName, String instance, String job, String name,
        @Nullable String topic, @Nullable String partition) {
        _domainName = domainName;
        _instance = instance;
        _job = job;
        _name = name;
        _topic = topic;
        _partition = partition;
    }

    public String domainName() {
        return _domainName;
    }

    public String instance() {
        return _instance;
    }

    public String job() {
        return _job;
    }

    public String name() {
        return _name;
    }

    @Nullable public String topic() {
        return _topic;
    }

    @Nullable public String partition() {
        return _partition;
    }

    @Override
    public String toString() {
        return "PrometheusMetric{" +
            "_domainName='" + _domainName + '\'' +
            ", _instance='" + _instance + '\'' +
            ", _job='" + _job + '\'' +
            ", _name='" + _name + '\'' +
            ", _topic='" + _topic + '\'' +
            ", _partition='" + _partition + '\'' +
            '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrometheusMetric that = (PrometheusMetric) o;
        return Objects.equals(_domainName, that._domainName) &&
            Objects.equals(_instance, that._instance) &&
            Objects.equals(_job, that._job) &&
            Objects.equals(_name, that._name) &&
            Objects.equals(_topic, that._topic) &&
            Objects.equals(_partition, that._partition);
    }

    @Override public int hashCode() {
        return Objects.hash(_domainName, _instance, _job, _name, _topic, _partition);
    }
}
