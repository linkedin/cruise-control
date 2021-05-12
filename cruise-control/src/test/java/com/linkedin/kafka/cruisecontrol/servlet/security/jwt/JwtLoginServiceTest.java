/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.jwt;

import com.linkedin.kafka.cruisecontrol.servlet.security.SecurityUtils;
import com.linkedin.kafka.cruisecontrol.servlet.security.UserStoreAuthorizationService;
import com.nimbusds.jwt.SignedJWT;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.server.UserIdentity;
import org.junit.Test;
import javax.servlet.http.HttpServletRequest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JwtLoginServiceTest {

  private static final String TEST_USER = "testUser";

  @Test
  public void testValidateTokenSuccessfully() throws Exception {
    UserStore testUserStore = new UserStore();
    testUserStore.addUser(TEST_USER, SecurityUtils.NO_CREDENTIAL, new String[] {"USER"});
    TokenGenerator.TokenAndKeys tokenAndKeys = TokenGenerator.generateToken(TEST_USER);
    JwtLoginService loginService = new JwtLoginService(new UserStoreAuthorizationService(testUserStore), tokenAndKeys.publicKey(), null);

    SignedJWT jwtToken = SignedJWT.parse(tokenAndKeys.token());
    HttpServletRequest request = mock(HttpServletRequest.class);
    expect(request.getAttribute(JwtAuthenticator.JWT_TOKEN_REQUEST_ATTRIBUTE)).andReturn(tokenAndKeys.token());

    replay(request);
    UserIdentity identity = loginService.login(TEST_USER, jwtToken, request);
    verify(request);
    assertNotNull(identity);
    assertEquals(TEST_USER, identity.getUserPrincipal().getName());
  }

  @Test
  public void testFailSignatureValidation() throws Exception {
    UserStore testUserStore = new UserStore();
    testUserStore.addUser(TEST_USER, SecurityUtils.NO_CREDENTIAL, new String[] {"USER"});
    TokenGenerator.TokenAndKeys tokenAndKeys = TokenGenerator.generateToken(TEST_USER);
    // This will be signed with a different key
    TokenGenerator.TokenAndKeys tokenAndKeys2 = TokenGenerator.generateToken(TEST_USER);
    JwtLoginService loginService = new JwtLoginService(new UserStoreAuthorizationService(testUserStore), tokenAndKeys2.publicKey(), null);

    SignedJWT jwtToken = SignedJWT.parse(tokenAndKeys.token());
    HttpServletRequest request = mock(HttpServletRequest.class);

    UserIdentity identity = loginService.login(TEST_USER, jwtToken, request);
    assertNull(identity);
  }

  @Test
  public void testFailAudienceValidation() throws Exception {
    UserStore testUserStore = new UserStore();
    testUserStore.addUser(TEST_USER, SecurityUtils.NO_CREDENTIAL, new String[] {"USER"});
    TokenGenerator.TokenAndKeys tokenAndKeys = TokenGenerator.generateToken(TEST_USER, Arrays.asList("A", "B"));
    JwtLoginService loginService = new JwtLoginService(
        new UserStoreAuthorizationService(testUserStore), tokenAndKeys.publicKey(), Arrays.asList("C", "D"));

    SignedJWT jwtToken = SignedJWT.parse(tokenAndKeys.token());
    HttpServletRequest request = mock(HttpServletRequest.class);

    UserIdentity identity = loginService.login(TEST_USER, jwtToken, request);
    assertNull(identity);
  }

  @Test
  public void testFailExpirationValidation() throws Exception {
    UserStore testUserStore = new UserStore();
    testUserStore.addUser(TEST_USER, SecurityUtils.NO_CREDENTIAL, new String[] {"USER"});
    TokenGenerator.TokenAndKeys tokenAndKeys = TokenGenerator.generateToken(TEST_USER, 1L);
    JwtLoginService loginService = new JwtLoginService(new UserStoreAuthorizationService(testUserStore), tokenAndKeys.publicKey(), null);

    SignedJWT jwtToken = SignedJWT.parse(tokenAndKeys.token());
    HttpServletRequest request = mock(HttpServletRequest.class);

    UserIdentity identity = loginService.login(TEST_USER, jwtToken, request);
    assertNull(identity);
  }

  @Test
  public void testRevalidateTokenPasses() throws Exception {
    UserStore testUserStore = new UserStore();
    testUserStore.addUser(TEST_USER, SecurityUtils.NO_CREDENTIAL, new String[] {"USER"});
    TokenGenerator.TokenAndKeys tokenAndKeys = TokenGenerator.generateToken(TEST_USER);
    JwtLoginService loginService = new JwtLoginService(new UserStoreAuthorizationService(testUserStore), tokenAndKeys.publicKey(), null);

    SignedJWT jwtToken = SignedJWT.parse(tokenAndKeys.token());
    HttpServletRequest request = mock(HttpServletRequest.class);
    expect(request.getAttribute(JwtAuthenticator.JWT_TOKEN_REQUEST_ATTRIBUTE)).andReturn(tokenAndKeys.token());

    replay(request);
    UserIdentity identity = loginService.login(TEST_USER, jwtToken, request);
    verify(request);
    assertNotNull(identity);
    assertEquals(TEST_USER, identity.getUserPrincipal().getName());
    assertTrue(loginService.validate(identity));
  }

  @Test
  public void testRevalidateTokenFails() throws Exception {
    UserStore testUserStore = new UserStore();
    testUserStore.addUser(TEST_USER, SecurityUtils.NO_CREDENTIAL, new String[] {"USER"});
    Instant now = Instant.now();
    TokenGenerator.TokenAndKeys tokenAndKeys = TokenGenerator.generateToken(TEST_USER, now.plusSeconds(10).toEpochMilli());
    Clock fixedClock = Clock.fixed(now, ZoneOffset.UTC);
    JwtLoginService loginService = new JwtLoginService(
        new UserStoreAuthorizationService(testUserStore), tokenAndKeys.publicKey(), null, fixedClock);

    SignedJWT jwtToken = SignedJWT.parse(tokenAndKeys.token());
    HttpServletRequest request = mock(HttpServletRequest.class);
    expect(request.getAttribute(JwtAuthenticator.JWT_TOKEN_REQUEST_ATTRIBUTE)).andReturn(tokenAndKeys.token());

    replay(request);
    UserIdentity identity = loginService.login(TEST_USER, jwtToken, request);
    verify(request);
    assertNotNull(identity);
    assertEquals(TEST_USER, identity.getUserPrincipal().getName());
    loginService.setClock(Clock.offset(fixedClock, Duration.ofSeconds(20)));
    assertFalse(loginService.validate(identity));
  }
}
