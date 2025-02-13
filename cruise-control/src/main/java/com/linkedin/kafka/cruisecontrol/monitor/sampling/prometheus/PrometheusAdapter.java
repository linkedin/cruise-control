/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import static com.linkedin.cruisecontrol.common.utils.Utils.*;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.*;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusQueryResult;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

/**
 * This class provides an adapter to make queries to a Prometheus Server to fetch metric values.
 */
class PrometheusAdapter {
    private static final Gson GSON = new Gson();
    static final String QUERY_RANGE_API_PATH = "/api/v1/query_range";
    static final String SUCCESS = "success";
    private static final String QUERY = "query";
    private static final String START = "start";
    private static final String END = "end";
    private static final String STEP = "step";
    private static final String SERVICE_NAME = "aps";

    private final CloseableHttpClient _httpClient;
    protected final URIBuilder _prometheusEndpoint;
    protected final int _samplingIntervalMs;
    private final String _region;
    private final Boolean _use_sigv4;

    PrometheusAdapter(CloseableHttpClient httpClient,
        URIBuilder prometheusEndpoint,
                      int samplingIntervalMs,
                      String region) {
        _httpClient = validateNotNull(httpClient, "httpClient cannot be null.");
        _prometheusEndpoint = validateNotNull(prometheusEndpoint, "prometheusEndpoint cannot be null.");
        _samplingIntervalMs = samplingIntervalMs;
        _region = region;
        _use_sigv4 = region != null;
    }

    public int samplingIntervalMs() {
        return _samplingIntervalMs;
    }

    public List<PrometheusQueryResult> queryMetric(String queryString,
                                                   long startTimeMs,
                                                   long endTimeMs) throws IOException {


        List<NameValuePair> data = new ArrayList<>();
        data.add(new BasicNameValuePair(QUERY, queryString));
        /* "start" and "end" are expected to be unix timestamp in seconds (number of seconds since the Unix epoch).
         They accept values with a decimal point (up to 64 bits). The samples returned are inclusive of the "end"
         timestamp provided.
         */
        data.add(new BasicNameValuePair(START, String.valueOf((double) startTimeMs / SEC_TO_MS)));
        data.add(new BasicNameValuePair(END, String.valueOf((double) endTimeMs / SEC_TO_MS)));
        // step is expected to be in seconds, and accept values with a decimal point (up to 64 bits).
        data.add(new BasicNameValuePair(STEP, String.valueOf((double) _samplingIntervalMs / SEC_TO_MS)));

        String queryParams = URLEncodedUtils.format(data, StandardCharsets.UTF_8);
        URI queryUri = URI.create(_prometheusEndpoint.toString() + QUERY_RANGE_API_PATH + "?" + queryParams);
        HttpPost httpPost = new HttpPost(queryUri);

        // Sign the request if SigV4 is enabled
        if (_use_sigv4) {
            signRequest(httpPost, queryUri);
        }

        try (CloseableHttpResponse response = _httpClient.execute(httpPost)) {
            int responseCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            InputStream content = entity.getContent();
            String responseString = IOUtils.toString(content, StandardCharsets.UTF_8);
            if (responseCode != HttpServletResponse.SC_OK) {
                throw new IOException(String.format("Received non-success response code on Prometheus API HTTP call,"
                                                    + " response code = %d, response body = %s",
                                                    responseCode, responseString));
            }
            PrometheusResponse prometheusResponse = GSON.fromJson(responseString, PrometheusResponse.class);
            if (prometheusResponse == null) {
                throw new IOException(String.format(
                    "No response received from Prometheus API query, response body = %s", responseString));
            }

            if (!SUCCESS.equals(prometheusResponse.status())) {
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
    
    private void signRequest(HttpPost httpPost, URI queryUri) throws IOException {
        // Get AWS credentials
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        AwsCredentials credentials = credentialsProvider.resolveCredentials();

        // Create the HTTP request
        SdkHttpFullRequest.Builder httpRequest = SdkHttpFullRequest.builder()
            .uri(queryUri)
            .method(SdkHttpMethod.POST)
            .putHeader("Content-Type", "application/text");

        AwsV4HttpSigner signer = AwsV4HttpSigner.create();

        SignedRequest signedRequest =
            signer.sign(r -> r.identity(credentials)
                .request(httpRequest.build())
                .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, SERVICE_NAME)
                .putProperty(AwsV4HttpSigner.REGION_NAME, _region));

        signedRequest.request().headers().forEach((key, values) ->
            httpPost.setHeader(key, values.get(0)));

    }
}
