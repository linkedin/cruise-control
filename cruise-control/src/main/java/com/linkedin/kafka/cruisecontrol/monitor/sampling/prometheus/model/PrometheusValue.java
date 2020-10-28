/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model;

import java.util.Objects;

import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;

@JsonAdapter(PrometheusValueDeserializer.class)
public class PrometheusValue {
    @SerializedName("epochSeconds")
    private final long _epochSeconds;
    @SerializedName("value")
    private final double _value;

    public PrometheusValue(final long epochSeconds, final double value) {
        this._epochSeconds = epochSeconds;
        this._value = value;
    }

    public long epochSeconds() {
        return _epochSeconds;
    }

    public double value() {
        return _value;
    }

    @Override
    public String toString() {
        return "PrometheusValue{" +
            "_epochSeconds=" + _epochSeconds +
            ", _value=" + _value +
            '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrometheusValue that = (PrometheusValue) o;
        return _epochSeconds == that._epochSeconds &&
            Double.compare(that._value, _value) == 0;
    }

    @Override public int hashCode() {
        return Objects.hash(_epochSeconds, _value);
    }
}
