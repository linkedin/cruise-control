/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.cruisecontrol.http;

import io.vertx.core.MultiMap;
import java.io.IOException;
import java.util.Map;

/**
 * Request context for cruise control
 */
public interface CruiseControlRequestContext {

    String[] HEADERS_TO_TRY = {
            "X-Forwarded-For",
            "Proxy-Client-IP",
            "WL-Proxy-Client-IP",
            "HTTP_X_FORWARDED_FOR",
            "HTTP_X_FORWARDED",
            "HTTP_X_CLUSTER_CLIENT_IP",
            "HTTP_CLIENT_IP",
            "HTTP_FORWARDED_FOR",
            "HTTP_FORWARDED",
            "HTTP_VIA",
            "REMOTE_ADDR"
    };

    String getRequestURL();

    String getUserTaskIdString();

    String getMethod();

    String getPathInfo();

    String getClientIdentity();

    MultiMap getHeaders();

    String getHeader(String header);

    String getRemoteAddr();

    String getUserPrincipal();

    Map<String, String[]> getParameterMap();

    String getRequestUri();

    String getParameter(String parameter);

    void writeResponseToOutputStream(int responseCode,
                                     boolean json,
                                     boolean wantJsonSchema,
                                     String responseMessage) throws IOException;

    CruiseControlHttpSession getSession();

    String getRequestURI();

    /**
     * Returns the body in JSON.
     * @return the body in JSON
     * @throws IOException
     */
    Map<String, Object> getJson() throws IOException;

    void setHeader(String key, String value);

    Map<String, Object> getParameterConfigOverrides();
}
