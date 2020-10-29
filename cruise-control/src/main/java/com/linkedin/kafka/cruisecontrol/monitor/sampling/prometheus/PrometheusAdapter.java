/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;

import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusQueryResult;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusResponse;

/**
 * This class provides an adapter to make queries to a Prometheus Server to fetch metric values.
 */
class PrometheusAdapter {
    private static final int MILLIS_IN_SECOND = 1000;
    private static final Gson GSON = new Gson();

    private final CloseableHttpClient _httpClient;
    /* Visible for testing */
    final HttpHost _prometheusEndpoint;
    /* Visible for testing */
    final Integer _samplingIntervalMs;

    public PrometheusAdapter(CloseableHttpClient httpClient,
                             HttpHost prometheusEndpoint,
                             Integer samplingIntervalMs) {
        _httpClient = httpClient;
        _prometheusEndpoint = prometheusEndpoint;
        _samplingIntervalMs = samplingIntervalMs;
    }

    public List<PrometheusQueryResult> queryMetric(String queryString,
                                                   long startTimeMs,
                                                   long endTimeMs) throws IOException {
        URI queryUri = URI.create(_prometheusEndpoint.toURI() + "/api/v1/query_range");
        HttpPost httpPost = new HttpPost(queryUri);

        List<NameValuePair> data = new ArrayList<>();
        data.add(new BasicNameValuePair("query", queryString));
        data.add(new BasicNameValuePair("start", String.valueOf(startTimeMs / MILLIS_IN_SECOND)));
        data.add(new BasicNameValuePair("end", String.valueOf(endTimeMs / MILLIS_IN_SECOND)));
        data.add(new BasicNameValuePair("step", String.valueOf(_samplingIntervalMs / MILLIS_IN_SECOND)));

        httpPost.setEntity(new UrlEncodedFormEntity(data));
        try (CloseableHttpResponse response = _httpClient.execute(httpPost)) {
            int responseCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            InputStream content = entity.getContent();
            String responseString = IOUtils.toString(content, StandardCharsets.UTF_8);
            if (responseCode != 200) {
                throw new IOException(String.format("Received non-success response code on Prometheus API HTTP call,"
                                                    + " response code = %s, response body = %s",
                                                    responseCode, responseString));
            }
            PrometheusResponse prometheusResponse = GSON.fromJson(responseString, PrometheusResponse.class);
            if (prometheusResponse == null) {
                throw new IOException(String.format(
                    "No response received from Prometheus API query, response body = %s", responseString));
            }

            if (!"success".equals(prometheusResponse.status())) {
                throw new IOException(String.format(
                    "Prometheus API query was not successful, response body = %s", responseString));
            }
            if (prometheusResponse.data() == null
                || prometheusResponse.data().result() == null) {
                throw new IOException(String.format(
                    "Response from Prometheus HTTP API is malformed, response body = %s", responseString));
            }
            EntityUtils.consume(entity);
            return prometheusResponse.data().result();
        }
    }
}
