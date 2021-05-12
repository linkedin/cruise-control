/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import com.linkedin.kafka.cruisecontrol.CruiseControlIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import org.apache.http.auth.BasicUserPrincipal;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.DefaultUserIdentity;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Credential;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STATE;
import static org.junit.Assert.assertEquals;

public class AuthenticationIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final String TEST_USER = "test";
  private static final String TEST_PASSWORD = "12345";
  private static final String TEST_BAD_PASSWORD = "bad_password";
  private static final String ADMIN_ROLE = "admin";
  private static final String CRUISE_CONTROL_STATE_ENDPOINT = "kafkacruisecontrol/" + STATE;
  private static final String ANY_PATH = "/*";

  @Before
  public void setup() throws Exception {
    super.start();
  }

  @After
  public void teardown() {
    super.stop();
  }

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG, true);
    configs.put(WebServerConfig.WEBSERVER_SECURITY_PROVIDER_CONFIG, DummySecurityProvider.class.getName());
    return configs;
  }

  @Test
  public void testSuccessfulAuthentication() throws IOException, URISyntaxException {
    HttpURLConnection connection = (HttpURLConnection) new URI(_app.serverUrl())
        .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();
    String encoded = Base64.getEncoder().encodeToString((TEST_USER + ":" + TEST_PASSWORD).getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty(HttpHeader.AUTHORIZATION.asString(), "Basic " + encoded);
    assertEquals(HttpServletResponse.SC_OK, connection.getResponseCode());
  }

  @Test
  public void testUnsuccessfulAuthentication() throws IOException, URISyntaxException {
    HttpURLConnection connection = (HttpURLConnection) new URI(_app.serverUrl())
        .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();
    String encoded = Base64.getEncoder().encodeToString((TEST_USER + ":" + TEST_BAD_PASSWORD).getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty(HttpHeader.AUTHORIZATION.asString(), "Basic " + encoded);
    assertEquals(HttpServletResponse.SC_UNAUTHORIZED, connection.getResponseCode());
  }

  public static class DummySecurityProvider implements SecurityProvider {

    @Override
    public void init(KafkaCruiseControlConfig config) { }

    @Override
    public List<ConstraintMapping> constraintMappings() {
      ConstraintMapping mapping = new ConstraintMapping();
      Constraint constraint = new Constraint();
      constraint.setAuthenticate(true);
      constraint.setName(Constraint.__BASIC_AUTH);
      constraint.setRoles(new String[] { ADMIN_ROLE });
      mapping.setConstraint(constraint);
      mapping.setPathSpec(ANY_PATH);

      return Collections.singletonList(mapping);
    }

    @Override
    public LoginService loginService() {
      return new ConstantLoginService();
    }

    @Override
    public Authenticator authenticator() {
      return new BasicAuthenticator();
    }

    @Override
    public Set<String> roles() {
      return Collections.singleton(ADMIN_ROLE);
    }

    private static class ConstantLoginService implements LoginService {

      private static final UserIdentity USER_IDENTITY = new DefaultUserIdentity(
          new Subject(true,
              Collections.singleton(new BasicUserPrincipal(TEST_USER)),
              Collections.emptySet(),
              Collections.singleton(Credential.getCredential(TEST_PASSWORD))),
          new BasicUserPrincipal(TEST_USER),
          new String[] { ADMIN_ROLE });

      private IdentityService _identityService = new DefaultIdentityService();

      @Override
      public String getName() {
        return null;
      }

      @Override
      public UserIdentity login(String username, Object credentials, ServletRequest request) {
        return TEST_USER.equals(username) && TEST_PASSWORD.equals(credentials) ? USER_IDENTITY : null;
      }

      @Override
      public boolean validate(UserIdentity user) {
        return USER_IDENTITY.equals(user);
      }

      @Override
      public IdentityService getIdentityService() {
        return _identityService;
      }

      @Override
      public void setIdentityService(IdentityService service) {
        this._identityService = service;
      }

      @Override
      public void logout(UserIdentity user) {

      }
    }
  }
}
