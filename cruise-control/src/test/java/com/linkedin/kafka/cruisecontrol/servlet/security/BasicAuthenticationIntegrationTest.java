/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import com.linkedin.kafka.cruisecontrol.CruiseControlIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import org.eclipse.jetty.http.HttpHeader;
import org.junit.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STATE;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STOP_PROPOSAL_EXECUTION;
import static org.junit.Assert.assertEquals;

public class BasicAuthenticationIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final String CRUISE_CONTROL_STATE_ENDPOINT = "kafkacruisecontrol/" + STATE;
  private static final String CRUISE_CONTROL_PAUSE_SAMPLING_ENDPOINT = "kafkacruisecontrol/" + STOP_PROPOSAL_EXECUTION;

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG, true);
    configs.put(WebServerConfig.BASIC_AUTH_CREDENTIALS_FILE_CONFIG,
        Objects.requireNonNull(this.getClass().getClassLoader().getResource("basic-auth.credentials")).getPath());
    return configs;
  }

  @Test
  public void testBasicAuthenticationAsUser() throws IOException, URISyntaxException {
    HttpURLConnection stateEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
        .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();
    String userCreds = Base64.getEncoder().encodeToString(("ccTestUser" + ":" + "TestPwd123").getBytes(StandardCharsets.UTF_8));
    stateEndpointConnection.setRequestProperty(HttpHeader.AUTHORIZATION.asString(), "Basic " + userCreds);
    assertEquals(HttpServletResponse.SC_OK, stateEndpointConnection.getResponseCode());

    HttpURLConnection samplingEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
        .resolve(CRUISE_CONTROL_PAUSE_SAMPLING_ENDPOINT).toURL().openConnection();
    samplingEndpointConnection.setRequestMethod("POST");
    samplingEndpointConnection.setRequestProperty(HttpHeader.AUTHORIZATION.asString(), "Basic " + userCreds);
    assertEquals(HttpServletResponse.SC_FORBIDDEN, samplingEndpointConnection.getResponseCode());
  }

  @Test
  public void testBasicAuthenticationAsAdmin() throws IOException, URISyntaxException {
    HttpURLConnection stateEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
        .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();
    String adminCreds = Base64.getEncoder().encodeToString(("ccTestAdmin" + ":" + "TestPass123").getBytes(StandardCharsets.UTF_8));
    stateEndpointConnection.setRequestProperty(HttpHeader.AUTHORIZATION.asString(), "Basic " + adminCreds);
    assertEquals(HttpServletResponse.SC_OK, stateEndpointConnection.getResponseCode());

    HttpURLConnection samplingEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
        .resolve(CRUISE_CONTROL_PAUSE_SAMPLING_ENDPOINT).toURL().openConnection();
    samplingEndpointConnection.setRequestMethod("POST");
    samplingEndpointConnection.setRequestProperty(HttpHeader.AUTHORIZATION.asString(), "Basic " + adminCreds);
    assertEquals(HttpServletResponse.SC_OK, samplingEndpointConnection.getResponseCode());
  }
}
