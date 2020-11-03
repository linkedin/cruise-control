/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.localserver.LocalServerTestBase;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.junit.Test;

import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusMetric;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusQueryResult;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusValue;

import static org.junit.Assert.assertEquals;

public class PrometheusAdapterTest extends LocalServerTestBase {
    private static final long START_TIME_MS = 1603301400000L;
    private static final long END_TIME_MS = 1603301459000L;

    @Test
    public void testSuccessfulResponseDeserialized() throws Exception {
        this.serverBootstrap.registerHandler("/api/v1/query_range", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(200);
                response.setEntity(buildSuccessResponseEntity());
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, 30000);
        final List<PrometheusQueryResult> prometheusQueryResults = prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);

        assertEquals(expectedResults().toString(), prometheusQueryResults.toString());
        assertEquals(expectedResults(), prometheusQueryResults);
    }

    private HttpEntity buildSuccessResponseEntity() {
        return new StringEntity("{\n"
            + "    \"status\": \"success\",\n"
            + "    \"data\": {\n"
            + "        \"resultType\": \"matrix\",\n"
            + "        \"result\": [\n"
            + "            {\n"
            + "                \"metric\": {\n"
            + "                    \"__name__\": \"kafka_server_BrokerTopicMetrics_OneMinuteRate\",\n"
            + "                    \"instance\": \"b-1.test-cluster.org:11001\",\n"
            + "                    \"job\": \"jmx\",\n"
            + "                    \"name\": \"BytesOutPerSec\"\n"
            + "                },\n"
            + "                \"values\": [\n"
            + "                    [\n"
            + "                        1603301400,\n"
            + "                        \"1024\"\n"
            + "                    ],\n"
            + "                    [\n"
            + "                        1603301430,\n"
            + "                        \"2048\"\n"
            + "                    ]\n"
            + "                ]\n"
            + "            },\n"
            + "            {\n"
            + "                \"metric\": {\n"
            + "                    \"__name__\": \"kafka_server_BrokerTopicMetrics_OneMinuteRate\",\n"
            + "                    \"instance\": \"b-2.test-cluster.org:11001\",\n"
            + "                    \"job\": \"jmx\",\n"
            + "                    \"name\": \"BytesOutPerSec\"\n"
            + "                },\n"
            + "                \"values\": [\n"
            + "                    [\n"
            + "                        1603301400,\n"
            + "                        \"4096\"\n"
            + "                    ],\n"
            + "                    [\n"
            + "                        1603301430,\n"
            + "                        \"4096\"\n"
            + "                    ]\n"
            + "                ]\n"
            + "            }\n"
            + "        ]\n"
            + "    }\n"
            + "}", StandardCharsets.UTF_8);
    }

    private List<PrometheusQueryResult> expectedResults() {
        return Arrays.asList(
            new PrometheusQueryResult(
                new PrometheusMetric(
                    "b-1.test-cluster.org:11001",
                    null, null),
                Arrays.asList(
                    new PrometheusValue(1603301400L, 1024),
                    new PrometheusValue(1603301430L, 2048)
                )
            ),
            new PrometheusQueryResult(
                new PrometheusMetric(
                    "b-2.test-cluster.org:11001",
                    null, null),
                Arrays.asList(
                    new PrometheusValue(1603301400L, 4096),
                    new PrometheusValue(1603301430L, 4096)
                )
            )
        );
    }

    @Test(expected = IOException.class)
    public void testFailureResponseWith200Code() throws Exception {
        this.serverBootstrap.registerHandler("/api/v1/query_range", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(200);
                response.setEntity(new StringEntity(
                    "{\"status\": \"failure\", \"data\": {\"result\": []}}", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, 30000);

        prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);
    }

    @Test(expected = IOException.class)
    public void testFailureResponseWith403Code() throws Exception {
        this.serverBootstrap.registerHandler("/api/v1/query_range", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(403);
                response.setEntity(new StringEntity(
                    "{\"status\": \"failure\", \"data\": {\"result\": []}}", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, 30000);

        prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);
    }

    @Test(expected = IOException.class)
    public void testEmptyResponse() throws Exception {
        this.serverBootstrap.registerHandler("/api/v1/query_range", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(200);
                response.setEntity(new StringEntity(
                    "", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, 30000);

        prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);
    }

    @Test(expected = IOException.class)
    public void testEmptyStatus() throws Exception {
        this.serverBootstrap.registerHandler("/api/v1/query_range", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(200);
                response.setEntity(new StringEntity(
                    "{\"data\":{\"result\": []}}", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, 30000);

        prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);
    }

    @Test(expected = IOException.class)
    public void testEmptyData() throws Exception {
        this.serverBootstrap.registerHandler("/api/v1/query_range", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(200);
                response.setEntity(new StringEntity(
                    "{\"status\":\"success\"}", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, 30000);

        prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);
    }

    @Test(expected = IOException.class)
    public void testEmptyResult() throws Exception {
        this.serverBootstrap.registerHandler("/api/v1/query_range", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(200);
                response.setEntity(new StringEntity(
                    "{\"status\": \"success\", \"data\": {}}", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, 30000);

        prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);
    }
}
