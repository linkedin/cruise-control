/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.servlet.security.jwt;

import com.linkedin.kafka.cruisecontrol.servlet.security.SecurityUtils;
import com.linkedin.kafka.cruisecontrol.servlet.security.UserStoreAuthorizationService;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.UserAuthentication;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Request;
import org.junit.Test;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class JwtAuthenticatorTest {

  private static final String TEST_USER = "testUser";
  private static final String TEST_USER_2 = "testUser2";
  private static final String JWT_TOKEN = "jwt_token";
  private static final String EXPECTED_TOKEN = "token";
  private static final String RANDOM_COOKIE_NAME = "random_cookie_name";
  private static final String TOKEN_PROVIDER = "http://mytokenprovider.com?origin=" + JwtAuthenticator.REDIRECT_URL;
  private static final String CRUISE_CONTROL_ENDPOINT = "http://cruisecontrol.mycompany.com/state";
  private static final String USER_ROLE = "USER";
  private static final String BASIC_SCHEME = "Basic";

  @Test
  public void testParseTokenFromAuthHeader() {
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    HttpServletRequest request = mock(HttpServletRequest.class);
    expect(request.getHeader(HttpHeader.AUTHORIZATION.asString())).andReturn(JwtAuthenticator.BEARER + " " + EXPECTED_TOKEN);
    replay(request);
    String actualToken = authenticator.getJwtFromBearerAuthorization(request);
    verify(request);
    assertEquals(EXPECTED_TOKEN, actualToken);
  }

  @Test
  public void testParseTokenFromAuthHeaderNoBearer() {
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    HttpServletRequest request = mock(HttpServletRequest.class);
    expect(request.getHeader(HttpHeader.AUTHORIZATION.asString())).andReturn(BASIC_SCHEME + " " + EXPECTED_TOKEN);
    replay(request);
    String actualToken = authenticator.getJwtFromBearerAuthorization(request);
    verify(request);
    assertNull(actualToken);
  }

  @Test
  public void testParseTokenFromCookie() {
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    HttpServletRequest request = mock(HttpServletRequest.class);
    expect(request.getCookies()).andReturn(new Cookie[] {new Cookie(JWT_TOKEN, EXPECTED_TOKEN)});
    replay(request);
    String actualToken = authenticator.getJwtFromCookie(request);
    verify(request);
    assertEquals(EXPECTED_TOKEN, actualToken);
  }

  @Test
  public void testParseTokenFromCookieNoJwtCookie() {
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    HttpServletRequest request = mock(HttpServletRequest.class);
    expect(request.getCookies()).andReturn(new Cookie[] {new Cookie(RANDOM_COOKIE_NAME, "")});
    replay(request);
    String actualToken = authenticator.getJwtFromCookie(request);
    verify(request);
    assertNull(actualToken);
  }

  @Test
  public void testRedirect() throws IOException, ServerAuthException {
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);

    HttpServletRequest request = mock(HttpServletRequest.class);
    expect(request.getMethod()).andReturn(HttpMethod.GET.asString());
    expect(request.getQueryString()).andReturn(null);
    expect(request.getHeader(HttpHeader.AUTHORIZATION.asString())).andReturn(null);
    expect(request.getCookies()).andReturn(new Cookie[] {});
    expect(request.getRequestURL()).andReturn(new StringBuffer(CRUISE_CONTROL_ENDPOINT));

    HttpServletResponse response = mock(HttpServletResponse.class);
    response.sendRedirect(TOKEN_PROVIDER.replace(JwtAuthenticator.REDIRECT_URL, CRUISE_CONTROL_ENDPOINT));
    expectLastCall().andVoid();

    replay(request, response);
    Authentication actualAuthentication = authenticator.validateRequest(request, response, true);
    verify(request, response);
    assertEquals(Authentication.SEND_CONTINUE, actualAuthentication);
  }

  @Test
  public void testSuccessfulLogin() throws Exception {
    UserStore testUserStore = new UserStore();
    testUserStore.addUser(TEST_USER, SecurityUtils.NO_CREDENTIAL, new String[]{USER_ROLE});
    TokenGenerator.TokenAndKeys tokenAndKeys = TokenGenerator.generateToken(TEST_USER);
    JwtLoginService loginService = new JwtLoginService(new UserStoreAuthorizationService(testUserStore), tokenAndKeys.publicKey(), null);

    Authenticator.AuthConfiguration configuration = mock(Authenticator.AuthConfiguration.class);
    expect(configuration.getLoginService()).andReturn(loginService);
    expect(configuration.getIdentityService()).andReturn(new DefaultIdentityService());
    expect(configuration.isSessionRenewedOnAuthentication()).andReturn(true);

    Request request = niceMock(Request.class);
    expect(request.getMethod()).andReturn(HttpMethod.GET.asString());
    expect(request.getHeader(HttpHeader.AUTHORIZATION.asString())).andReturn(null);
    request.setAttribute(JwtAuthenticator.JWT_TOKEN_REQUEST_ATTRIBUTE, tokenAndKeys.token());
    expectLastCall().andVoid();
    expect(request.getCookies()).andReturn(new Cookie[] {new Cookie(JWT_TOKEN, tokenAndKeys.token())});
    expect(request.getAttribute(JwtAuthenticator.JWT_TOKEN_REQUEST_ATTRIBUTE)).andReturn(tokenAndKeys.token());

    HttpServletResponse response = mock(HttpServletResponse.class);

    replay(configuration, request, response);
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    authenticator.setConfiguration(configuration);
    UserAuthentication authentication = (UserAuthentication) authenticator.validateRequest(request, response, true);
    verify(configuration, request, response);

    assertNotNull(authentication);
    assertThat(authentication.getUserIdentity().getUserPrincipal(), instanceOf(JwtUserPrincipal.class));
    JwtUserPrincipal userPrincipal = (JwtUserPrincipal) authentication.getUserIdentity().getUserPrincipal();
    assertEquals(TEST_USER, userPrincipal.getName());
    assertEquals(tokenAndKeys.token(), userPrincipal.getSerializedToken());
  }

  @Test
  public void testFailedLoginWithUserNotFound() throws Exception {
    UserStore testUserStore = new UserStore();
    testUserStore.addUser(TEST_USER_2, SecurityUtils.NO_CREDENTIAL, new String[] {USER_ROLE});
    TokenGenerator.TokenAndKeys tokenAndKeys = TokenGenerator.generateToken(TEST_USER);
    JwtLoginService loginService = new JwtLoginService(new UserStoreAuthorizationService(testUserStore), tokenAndKeys.publicKey(), null);

    Authenticator.AuthConfiguration configuration = mock(Authenticator.AuthConfiguration.class);
    expect(configuration.getLoginService()).andReturn(loginService);
    expect(configuration.getIdentityService()).andReturn(new DefaultIdentityService());
    expect(configuration.isSessionRenewedOnAuthentication()).andReturn(true);

    Request request = niceMock(Request.class);
    expect(request.getMethod()).andReturn(HttpMethod.GET.asString());
    expect(request.getHeader(HttpHeader.AUTHORIZATION.asString())).andReturn(null);
    request.setAttribute(JwtAuthenticator.JWT_TOKEN_REQUEST_ATTRIBUTE, tokenAndKeys.token());
    expectLastCall().andVoid();
    expect(request.getCookies()).andReturn(new Cookie[] {new Cookie(JWT_TOKEN, tokenAndKeys.token())});
    expect(request.getAttribute(JwtAuthenticator.JWT_TOKEN_REQUEST_ATTRIBUTE)).andReturn(tokenAndKeys.token());

    HttpServletResponse response = mock(HttpServletResponse.class);
    response.setStatus(HttpStatus.UNAUTHORIZED_401);
    expectLastCall().andVoid();

    replay(configuration, request, response);
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    authenticator.setConfiguration(configuration);
    Authentication authentication = authenticator.validateRequest(request, response, true);
    verify(configuration, request, response);

    assertNotNull(authentication);
    assertEquals(Authentication.SEND_FAILURE, authentication);
  }

  @Test
  public void testFailedLoginWithInvalidToken() throws Exception {
    UserStore testUserStore = new UserStore();
    testUserStore.addUser(TEST_USER_2, SecurityUtils.NO_CREDENTIAL, new String[] {USER_ROLE});
    TokenGenerator.TokenAndKeys tokenAndKeys = TokenGenerator.generateToken(TEST_USER);
    TokenGenerator.TokenAndKeys tokenAndKeys2 = TokenGenerator.generateToken(TEST_USER);
    JwtLoginService loginService = new JwtLoginService(new UserStoreAuthorizationService(testUserStore), tokenAndKeys.publicKey(), null);

    Authenticator.AuthConfiguration configuration = mock(Authenticator.AuthConfiguration.class);
    expect(configuration.getLoginService()).andReturn(loginService);
    expect(configuration.getIdentityService()).andReturn(new DefaultIdentityService());
    expect(configuration.isSessionRenewedOnAuthentication()).andReturn(true);

    Request request = niceMock(Request.class);
    expect(request.getMethod()).andReturn(HttpMethod.GET.asString());
    expect(request.getHeader(HttpHeader.AUTHORIZATION.asString())).andReturn(null);
    request.setAttribute(JwtAuthenticator.JWT_TOKEN_REQUEST_ATTRIBUTE, tokenAndKeys2.token());
    expectLastCall().andVoid();
    expect(request.getCookies()).andReturn(new Cookie[] {new Cookie(JWT_TOKEN, tokenAndKeys2.token())});

    HttpServletResponse response = mock(HttpServletResponse.class);
    response.setStatus(HttpStatus.UNAUTHORIZED_401);
    expectLastCall().andVoid();

    replay(configuration, request, response);
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    authenticator.setConfiguration(configuration);
    Authentication authentication = authenticator.validateRequest(request, response, true);
    verify(configuration, request, response);

    assertNotNull(authentication);
    assertEquals(Authentication.SEND_FAILURE, authentication);
  }
}
