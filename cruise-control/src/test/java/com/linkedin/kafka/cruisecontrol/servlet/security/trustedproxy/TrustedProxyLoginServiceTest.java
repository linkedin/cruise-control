/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import com.linkedin.kafka.cruisecontrol.servlet.security.DefaultRoleSecurityProvider;
import com.linkedin.kafka.cruisecontrol.servlet.security.SecurityUtils;
import com.linkedin.kafka.cruisecontrol.servlet.security.spnego.SpnegoLoginServiceWithAuthServiceLifecycle;
import org.eclipse.jetty.security.SpnegoUserIdentity;
import org.eclipse.jetty.security.SpnegoUserPrincipal;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.security.authentication.AuthorizationService;
import org.eclipse.jetty.server.UserIdentity;
import org.junit.Test;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import java.util.Collections;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DO_AS;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TrustedProxyLoginServiceTest {

  public static final String TEST_SERVICE_USER = "testServiceUser";
  public static final String ENCODED_TOKEN = "encoded_token";
  public static final String TEST_USER = "testUser";

  private static class TestAuthorizer implements AuthorizationService {

    private final UserStore _adminUserStore = new UserStore();

    TestAuthorizer(String testUser) {
      _adminUserStore.addUser(testUser, SecurityUtils.NO_CREDENTIAL, new String[] { DefaultRoleSecurityProvider.ADMIN });
    }

    @Override
    public UserIdentity getUserIdentity(HttpServletRequest request, String name) {
      return _adminUserStore.getUserIdentity(name);
    }
  }

  private final SpnegoLoginServiceWithAuthServiceLifecycle _mockSpnegoLoginService = mock(SpnegoLoginServiceWithAuthServiceLifecycle.class);
  private final SpnegoLoginServiceWithAuthServiceLifecycle _mockFallbackLoginService = mock(SpnegoLoginServiceWithAuthServiceLifecycle.class);

  @Test
  public void testSuccessfulAuthentication() {
    SpnegoUserPrincipal servicePrincipal = new SpnegoUserPrincipal(TEST_SERVICE_USER, ENCODED_TOKEN);
    UserIdentity serviceDelegate = mock(UserIdentity.class);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    SpnegoUserIdentity result = new SpnegoUserIdentity(subject, servicePrincipal, serviceDelegate);
    expect(_mockSpnegoLoginService.login(anyString(), anyObject(), anyObject())).andReturn(result);

    TestAuthorizer userAuthorizer = new TestAuthorizer(TEST_USER);

    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    expect(mockRequest.getParameter(DO_AS)).andReturn(TEST_USER);

    replay(_mockSpnegoLoginService, mockRequest);

    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
            userAuthorizer, false);
    UserIdentity doAsIdentity = trustedProxyLoginService.login(null, ENCODED_TOKEN, mockRequest);
    assertNotNull(doAsIdentity);
    assertNotNull(doAsIdentity.getUserPrincipal());
    assertEquals(doAsIdentity.getUserPrincipal().getName(), TEST_USER);
    assertEquals(((TrustedProxyPrincipal) doAsIdentity.getUserPrincipal()).servicePrincipal(), servicePrincipal);
    verify(_mockSpnegoLoginService, mockRequest);
  }

  @Test
  public void testNoDoAsUser() {
    SpnegoUserPrincipal servicePrincipal = new SpnegoUserPrincipal(TEST_SERVICE_USER, ENCODED_TOKEN);
    UserIdentity serviceDelegate = mock(UserIdentity.class);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    SpnegoUserIdentity result = new SpnegoUserIdentity(subject, servicePrincipal, serviceDelegate);
    expect(_mockSpnegoLoginService.login(anyString(), anyObject(), anyObject())).andReturn(result);

    TestAuthorizer userAuthorizer = new TestAuthorizer(TEST_USER);

    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    replay(_mockSpnegoLoginService);

    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
            userAuthorizer, false);
    UserIdentity doAsIdentity = trustedProxyLoginService.login(null, ENCODED_TOKEN, mockRequest);
    assertNotNull(doAsIdentity);
    assertNotNull(doAsIdentity.getUserPrincipal());
    assertNull(doAsIdentity.getUserPrincipal().getName());
    assertFalse(((SpnegoUserIdentity) doAsIdentity).isEstablished());
    verify(_mockSpnegoLoginService);
  }

  @Test
  public void testInvalidAuthServiceUser() {
    SpnegoUserPrincipal servicePrincipal = new SpnegoUserPrincipal(TEST_SERVICE_USER, ENCODED_TOKEN);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    SpnegoUserIdentity result = new SpnegoUserIdentity(subject, servicePrincipal, null);
    expect(_mockSpnegoLoginService.login(anyString(), anyObject(), anyObject())).andReturn(result);

    TestAuthorizer userAuthorizer = new TestAuthorizer(TEST_USER);

    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    expect(mockRequest.getParameter(DO_AS)).andReturn(TEST_USER);
    replay(_mockSpnegoLoginService);

    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
            userAuthorizer, false);
    UserIdentity doAsIdentity = trustedProxyLoginService.login(null, ENCODED_TOKEN, mockRequest);
    assertNotNull(doAsIdentity);
    assertFalse(((SpnegoUserIdentity) doAsIdentity).isEstablished());
  }

  @Test
  public void testFallbackToSpnego() {
    SpnegoUserPrincipal servicePrincipal = new SpnegoUserPrincipal(TEST_SERVICE_USER, ENCODED_TOKEN);
    UserIdentity serviceDelegate = mock(UserIdentity.class);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    SpnegoUserIdentity result = new SpnegoUserIdentity(subject, servicePrincipal, serviceDelegate);
    expect(_mockFallbackLoginService.login(anyString(), anyObject(), anyObject())).andReturn(result);

    TestAuthorizer userAuthorizer = new TestAuthorizer(TEST_USER);

    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    replay(_mockFallbackLoginService);

    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
            userAuthorizer, true);
    UserIdentity doAsIdentity = trustedProxyLoginService.login(null, ENCODED_TOKEN, mockRequest);
    assertNotNull(doAsIdentity);
    assertNotNull(doAsIdentity.getUserPrincipal());
    assertEquals(servicePrincipal, doAsIdentity.getUserPrincipal());
    verify(_mockFallbackLoginService);
  }

  @Test
  public void testTrustedProxyWithKerberosRules() {
    String username = "user1";
    String proxy = "proxy2@realm";
    SpnegoUserPrincipal servicePrincipal = new SpnegoUserPrincipal(proxy, ENCODED_TOKEN);
    UserIdentity serviceDelegate = mock(UserIdentity.class);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    SpnegoUserIdentity result = new SpnegoUserIdentity(subject, servicePrincipal, serviceDelegate);
    expect(_mockSpnegoLoginService.login(anyString(), anyObject(), anyObject())).andReturn(result);

    TestAuthorizer userAuthorizer = new TestAuthorizer(username);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    expect(mockRequest.getParameter(DO_AS)).andReturn(username);
    replay(_mockSpnegoLoginService, mockRequest);
    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
            userAuthorizer, false);

    UserIdentity doAsIdentity = trustedProxyLoginService.login(proxy, ENCODED_TOKEN, mockRequest);

    assertNotNull(doAsIdentity);
    assertNotNull(doAsIdentity.getUserPrincipal());
    assertEquals(doAsIdentity.getUserPrincipal().getName(), username);
    assertEquals(((TrustedProxyPrincipal) doAsIdentity.getUserPrincipal()).servicePrincipal(), servicePrincipal);
    verify(_mockSpnegoLoginService, mockRequest);
  }

  @Test
  public void testFallbackToSpnegoWithKerberosRules() {
    String username = "user1";
    String principal = "user1@realm";
    String usernameReplaced = username + "foo";
    SpnegoUserPrincipal servicePrincipal = new SpnegoUserPrincipal(usernameReplaced, ENCODED_TOKEN);
    UserIdentity serviceDelegate = mock(UserIdentity.class);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    SpnegoUserIdentity result = new SpnegoUserIdentity(subject, servicePrincipal, serviceDelegate);
    expect(_mockFallbackLoginService.login(anyString(), anyObject(), anyObject())).andReturn(result);

    TestAuthorizer userAuthorizer = new TestAuthorizer(username);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    replay(_mockFallbackLoginService);
    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
            userAuthorizer, true);

    UserIdentity doAsIdentity = trustedProxyLoginService.login(principal, ENCODED_TOKEN, mockRequest);

    assertNotNull(doAsIdentity);
    assertNotNull(doAsIdentity.getUserPrincipal());
    SpnegoUserPrincipal doAsPrincipal = (SpnegoUserPrincipal) doAsIdentity.getUserPrincipal();
    assertEquals(servicePrincipal.getName(), doAsPrincipal.getName());
    assertTrue(((SpnegoUserIdentity) doAsIdentity).isEstablished());
    verify(_mockFallbackLoginService);
  }

}
