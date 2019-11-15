/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.kafka.cruisecontrol.CruiseControlIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import org.apache.http.auth.BasicUserPrincipal;
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
import org.junit.Test;

import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AuthenticationIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final String TEST_USER = "test";
  private static final String TEST_PASSWORD = "12345";

  @Override
  protected Map<String, Object> withConfigs() {
    return Collections.singletonMap(WebServerConfig.SECURITY_PROVIDER_CONFIG, BasicSecurityProvider.class.getName());
  }

  @Test
  public void testSuccessfulAuthentication() throws IOException, URISyntaxException {
    HttpURLConnection connection = (HttpURLConnection) new URI(_app.serverUrl())
        .resolve("kafkacruisecontrol/state").toURL().openConnection();
    String encoded = Base64.getEncoder().encodeToString((TEST_USER + ":" + TEST_PASSWORD).getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + encoded);
    assertEquals(200, connection.getResponseCode());
  }

  @Test
  public void testUnsuccessfulAuthentication() throws IOException, URISyntaxException {
    HttpURLConnection connection = (HttpURLConnection) new URI(_app.serverUrl())
        .resolve("kafkacruisecontrol/state").toURL().openConnection();
    String encoded = Base64.getEncoder().encodeToString((TEST_USER + ":" + "bad_password").getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + encoded);
    assertEquals(401, connection.getResponseCode());
  }

  public static class BasicSecurityProvider implements SecurityProvider {

    @Override
    public List<ConstraintMapping> getConstraintMappings() {
      ConstraintMapping mapping = new ConstraintMapping();
      Constraint constraint = new Constraint();
      constraint.setAuthenticate(true);
      constraint.setName(Constraint.__BASIC_AUTH);
      constraint.setRoles(new String[] {"admin"});
      mapping.setConstraint(constraint);
      mapping.setPathSpec("/*");

      return Collections.singletonList(mapping);
    }

    @Override
    public LoginService getLoginService() {
      return new ConstantLoginService();
    }

    @Override
    public Authenticator getAuthenticator() {
      return new BasicAuthenticator();
    }

    @Override
    public Set<String> getRoles() {
      return Collections.singleton("admin");
    }

    private static class ConstantLoginService implements LoginService {

      private static final UserIdentity USER_IDENTITY = new DefaultUserIdentity(
          new Subject(true,
              Collections.singleton(new BasicUserPrincipal(TEST_USER)),
              Collections.emptySet(),
              Collections.singleton(Credential.getCredential(TEST_PASSWORD))),
          new BasicUserPrincipal(TEST_USER),
          new String[] {"admin"});

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
