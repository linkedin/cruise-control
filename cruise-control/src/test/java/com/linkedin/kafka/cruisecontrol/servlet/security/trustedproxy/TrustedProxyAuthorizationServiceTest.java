/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import org.eclipse.jetty.server.UserIdentity;
import org.junit.Test;
import javax.servlet.http.HttpServletRequest;
import java.util.Collections;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TrustedProxyAuthorizationServiceTest {

  private static final String AUTH_SERVICE_NAME = "authservice";
  private static final String IP_FILTER = "192\\.168\\.\\d{1,3}\\.\\d{1,3}";

  @Test
  public void testSuccessfulLoginWithIpFiltering() throws Exception {
    TrustedProxyAuthorizationService srv = new TrustedProxyAuthorizationService(Collections.singletonList(AUTH_SERVICE_NAME), IP_FILTER);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);

    expect(mockRequest.getRemoteAddr()).andReturn("192.168.0.1");
    replay(mockRequest);
    srv.start();
    try {
      UserIdentity result = srv.getUserIdentity(mockRequest, AUTH_SERVICE_NAME);
      assertNotNull(result);
      assertEquals(AUTH_SERVICE_NAME, result.getUserPrincipal().getName());
      verify(mockRequest);
    } finally {
      srv.stop();
    }
  }

  @Test
  public void testUnsuccessfulLoginWithIpFiltering() throws Exception {
    TrustedProxyAuthorizationService srv = new TrustedProxyAuthorizationService(Collections.singletonList(AUTH_SERVICE_NAME), IP_FILTER);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);

    expect(mockRequest.getRemoteAddr()).andReturn("192.167.0.1");
    replay(mockRequest);
    srv.start();
    try {
      UserIdentity result = srv.getUserIdentity(mockRequest, AUTH_SERVICE_NAME);
      assertNull(result);
      verify(mockRequest);
    } finally {
      srv.stop();
    }
  }

  @Test
  public void testSuccessfulLoginWithoutIpFiltering() throws Exception {
    TrustedProxyAuthorizationService srv = new TrustedProxyAuthorizationService(Collections.singletonList(AUTH_SERVICE_NAME), null);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    replay(mockRequest);
    srv.start();
    try {
      UserIdentity result = srv.getUserIdentity(mockRequest, AUTH_SERVICE_NAME);
      assertNotNull(result);
      assertEquals(AUTH_SERVICE_NAME, result.getUserPrincipal().getName());
      verify(mockRequest);
    } finally {
      srv.stop();
    }
  }
}
