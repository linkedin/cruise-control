/*
 * Copyright 2019 LinkedIn Corp.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.custom;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CustomAuthClient {
    private static final Logger LOG = LoggerFactory.getLogger(CustomAuthClient.class);

    private static CustomAuthClient instance;

    private final String _url;
    private final String _auth;
    private final String _scope;
    private final String _grantType;

    private static final String USER_NAME_PARAMETER_NAME = "username";
    private static final String PASSWORD_PARAMETER_NAME = "password";
    private static final String GRANT_TYPE_PARAMETER_NAME = "grant_type";
    private static final String SCOPE_PARAMETER_NAME = "scope";

    private static final String AUTHORIZATION_PROPERTY_NAME = "Authorization";
    private static final String CONTENT_TYPE_PROPERTY_NAME = "Content-Type";
    private static final String CONTENT_LENGTH_PROPERTY_NAME = "Content-Length";
    private static final String POST = "POST";

    private CustomAuthClient(String url, String auth, String scope, String grantType) {
        this._url = url;
        this._auth = auth;
        this._scope = scope;
        this._grantType = grantType;
    }

    /**
     * CustomAuthClient getInstance(String url, String auth, String scope, String grantType)
     * @return {@link CustomAuthClient}.
     */
    public static synchronized CustomAuthClient getInstance(String url, String auth, String scope, String grantType) {

        if (instance == null) {
            instance = new CustomAuthClient(url, auth, scope, grantType);
        }
        return instance;
    }

    /**
     * boolean verify(String username, String password)
     * @return true if authenticated.
     */
    public boolean verify(String username, String password) {
        Map<String, String> formData = Map.of(
                USER_NAME_PARAMETER_NAME, username,
                PASSWORD_PARAMETER_NAME, password,
                GRANT_TYPE_PARAMETER_NAME, _grantType,
                SCOPE_PARAMETER_NAME, _scope);

        try {
            String encodedData = getDataString(formData);
            URL u = new URL(_url);
            HttpURLConnection conn = (HttpURLConnection) u.openConnection();
            conn.setDoOutput(true);
            String basicAuth = "Basic " + _auth;
            conn.setRequestProperty(AUTHORIZATION_PROPERTY_NAME, basicAuth);
            conn.setRequestMethod(POST);
            conn.setRequestProperty(CONTENT_TYPE_PROPERTY_NAME, "application/x-www-form-urlencoded");
            conn.setRequestProperty(CONTENT_LENGTH_PROPERTY_NAME, String.valueOf(encodedData.length()));
            OutputStream os = conn.getOutputStream();
            os.write(encodedData.getBytes(UTF_8));

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
            if (first) {
                first = false;
            } else {
                result.append("&");
            }
            result.append(URLEncoder.encode(entry.getKey(), UTF_8));
            result.append("=");
            result.append(URLEncoder.encode(entry.getValue(), UTF_8));
        }
        return result.toString();
    }
}
