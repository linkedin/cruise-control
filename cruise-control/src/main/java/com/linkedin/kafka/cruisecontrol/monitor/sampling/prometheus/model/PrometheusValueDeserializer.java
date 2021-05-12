/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model;

import java.lang.reflect.Type;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

/**
 * Deserializer used to transform a metric value obtained from Prometheus'
 * query_range API response to the POJO {@link PrometheusValue}.
 *
 * The value is represented in the response as a one-dimensional array of
 * length of exactly two. The first element in the array is the timestamp
 * for the metric value in epoch seconds, and the second element is the
 * raw value which can have decimal points.
 */
class PrometheusValueDeserializer implements JsonDeserializer<PrometheusValue> {
    @Override
    public PrometheusValue deserialize(JsonElement json,
                                       Type typeOfT,
                                       JsonDeserializationContext context) throws JsonParseException {
        final JsonArray valueArray = json.getAsJsonArray();
        if (valueArray.size() != 2) {
            throw new JsonParseException("Every value array should have exactly two elements");
        }
        final long timestamp = valueArray.get(0).getAsLong();
        final String valueString = valueArray.get(1).getAsString();
        final double numericValue = Double.parseDouble(valueString);
        return new PrometheusValue(timestamp, numericValue);
    }
}
