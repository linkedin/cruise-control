/*
 * Copyright 2019 LinkedIn Corp.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.custom;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.AbstractGoal;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CustomAuthClient {
    private static final Logger LOG = LoggerFactory.getLogger(CustomAuthClient.class);

    private static CustomAuthClient instance;

    private final String url;
    private final String auth;
    private final String scope;
    private final String grantType;

    private static final String USER_NAME_PARAMETER_NAME = "username";
    private static final String PASSWORD_PARAMETER_NAME = "password";
    private static final String GRANT_TYPE_PARAMETER_NAME = "grant_type";
    private static final String SCOPE_PARAMETER_NAME = "scope";

    private static final String AUTHORIZATION_PROPERTY_NAME = "Authorization";
    private static final String CONTENT_TYPE_PROPERTY_NAME = "Content-Type";
    private static final String CONTENT_LENGTH_PROPERTY_NAME = "Content-Length";
    private static final String POST = "POST";

    private CustomAuthClient(String url, String auth, String scope, String grantType) {
        this.url = url;
        this.auth = auth;
        this.scope = scope;
        this.grantType = grantType;
    }

    public static synchronized CustomAuthClient getInstance(String url, String auth, String scope, String grantType) {

        if (instance == null) {
            instance = new CustomAuthClient(url, auth, scope, grantType);
        }
        return instance;
    }

    public boolean verify(String username, String password) {
        Map<String, String> formData = Map.of(
                USER_NAME_PARAMETER_NAME, username,
                PASSWORD_PARAMETER_NAME, password,
                GRANT_TYPE_PARAMETER_NAME, grantType,
                SCOPE_PARAMETER_NAME, scope);

        try {
            String encodedData = getDataString(formData);
            URL u = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) u.openConnection();
            conn.setDoOutput(true);
            String basicAuth = "Basic " + auth;
            conn.setRequestProperty(AUTHORIZATION_PROPERTY_NAME, basicAuth);
            conn.setRequestMethod(POST);
            conn.setRequestProperty(CONTENT_TYPE_PROPERTY_NAME, "application/x-www-form-urlencoded");
            conn.setRequestProperty(CONTENT_LENGTH_PROPERTY_NAME, String.valueOf(encodedData.length()));
            OutputStream os = conn.getOutputStream();
            os.write(encodedData.getBytes());

            if (conn.getResponseCode() == HttpStatus.SC_OK) {
                return true;
            }
        } catch (IOException e) {
            LOG.error("Authentication failed", e);
        }

        return false;
    }

    private static String getDataString(Map<String, String> params) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (first)
                first = false;
            else
                result.append("&");
            result.append(URLEncoder.encode(entry.getKey(), UTF_8));
            result.append("=");
            result.append(URLEncoder.encode(entry.getValue(), UTF_8));
        }
        return result.toString();
    }
}
