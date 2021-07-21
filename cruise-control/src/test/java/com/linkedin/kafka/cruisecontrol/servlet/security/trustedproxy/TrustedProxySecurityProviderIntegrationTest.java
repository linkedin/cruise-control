/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.security.SpnegoIntegrationTestHarness;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletResponse;
import java.net.HttpURLConnection;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STATE;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DO_AS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TrustedProxySecurityProviderIntegrationTest extends SpnegoIntegrationTestHarness {

  private static final String CRUISE_CONTROL_STATE_ENDPOINT = "kafkacruisecontrol/" + STATE;

  private static final String AUTH_SERVICE_NAME = "testauthservice";
  private static final String AUTH_SERVICE_PRINCIPAL = AUTH_SERVICE_NAME + "/localhost";

  public TrustedProxySecurityProviderIntegrationTest() throws KrbException {
  }

  @Override
  public List<String> principals() {
    List<String> principals = super.principals();
    principals.add(AUTH_SERVICE_PRINCIPAL);
    return principals;
  }

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> configs = super.withConfigs();
    configs.put(WebServerConfig.WEBSERVER_SECURITY_PROVIDER_CONFIG, TrustedProxySecurityProvider.class);
    configs.put(WebServerConfig.TRUSTED_PROXY_SERVICES_CONFIG, AUTH_SERVICE_NAME);

    return configs;
  }

  /**
   * Initializes the test environment.
   * @throws Exception
   */
  @Before
  public void setup() throws Exception {
    super.start();
  }

  /**
   * Stops the test environment.
   */
  @After
  public void teardown() {
    super.stop();
  }

  @Test
  public void testSuccessfulAuthentication() throws Exception {
    Subject subject = _miniKdc.loginAs(AUTH_SERVICE_PRINCIPAL);
    Subject.doAs(subject, (PrivilegedAction<Object>) () -> {
      try {
        String endpoint = CRUISE_CONTROL_STATE_ENDPOINT + "?" + DO_AS + "=" + CC_TEST_ADMIN;
        HttpURLConnection stateEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
        .resolve(endpoint).toURL().openConnection();
        assertEquals(HttpServletResponse.SC_OK, stateEndpointConnection.getResponseCode());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  @Test
  public void testNoDoAsParameter() throws Exception {
    Subject subject = _miniKdc.loginAs(AUTH_SERVICE_PRINCIPAL);
    Subject.doAs(subject, (PrivilegedAction<Object>) () -> {

      HttpURLConnection stateEndpointConnection;
      try {
        stateEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
            .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      // There is a bug in the Jetty implementation and it doesn't seem to handle the connection
      // properly in case of an error so it somehow doesn't send a response code. To work this around
      // I catch the RuntimeException that it throws.
      assertThrows(RuntimeException.class, stateEndpointConnection::getResponseCode);
      return null;
    });
  }

  @Test
  public void testNotAdminServiceLogin() throws Exception {
    Subject subject = _miniKdc.loginAs(SOME_OTHER_SERVICE_PRINCIPAL);
    Subject.doAs(subject, (PrivilegedAction<Object>) () -> {

        String endpoint = CRUISE_CONTROL_STATE_ENDPOINT + "?" + DO_AS + "=" + CC_TEST_ADMIN;
      HttpURLConnection stateEndpointConnection;
      try {
        stateEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
            .resolve(endpoint).toURL().openConnection();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      // There is a bug in the Jetty implementation and it doesn't seem to handle the connection
      // properly in case of an error so it somehow doesn't send a response code. To work this around
      // I catch the RuntimeException that it throws.
      assertThrows(RuntimeException.class, stateEndpointConnection::getResponseCode);
      return null;
    });
  }
}
