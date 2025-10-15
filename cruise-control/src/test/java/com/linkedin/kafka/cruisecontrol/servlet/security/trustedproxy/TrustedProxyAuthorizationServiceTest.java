/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import com.linkedin.kafka.cruisecontrol.servlet.security.DefaultRoleSecurityProvider;
import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.server.ConnectionMetaData;
import org.eclipse.jetty.server.Request;
import org.junit.Test;
import java.net.InetSocketAddress;
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
  private static final String ROLE = DefaultRoleSecurityProvider.ADMIN;

  @Test
  public void testSuccessfulLoginWithIpFiltering() throws Exception {
    TrustedProxyAuthorizationService srv = new TrustedProxyAuthorizationService(Collections.singletonList(AUTH_SERVICE_NAME), IP_FILTER);
    Request mockRequest = mock(Request.class);
    ConnectionMetaData mockConnectionMetaData = mock(ConnectionMetaData.class);

    expect(mockRequest.getConnectionMetaData()).andReturn(mockConnectionMetaData);
    expect(mockConnectionMetaData.getRemoteSocketAddress()).andReturn(new InetSocketAddress("192.168.0.1", 12345));
    replay(mockRequest, mockConnectionMetaData);
    srv.start();
    try {
      UserIdentity result = srv.getUserIdentity(mockRequest, AUTH_SERVICE_NAME);
      assertNotNull(result);
      assertEquals(AUTH_SERVICE_NAME, result.getUserPrincipal().getName());
      verify(mockRequest, mockConnectionMetaData);
    } finally {
      srv.stop();
    }
  }

  @Test
  public void testUnsuccessfulLoginWithIpFiltering() throws Exception {
    TrustedProxyAuthorizationService srv = new TrustedProxyAuthorizationService(Collections.singletonList(AUTH_SERVICE_NAME), IP_FILTER);
    Request mockRequest = mock(Request.class);
    ConnectionMetaData mockConnectionMetaData = mock(ConnectionMetaData.class);

    expect(mockRequest.getConnectionMetaData()).andReturn(mockConnectionMetaData);
    expect(mockConnectionMetaData.getRemoteSocketAddress()).andReturn(new InetSocketAddress("192.167.0.1", 12345));
    replay(mockRequest, mockConnectionMetaData);
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
    Request mockRequest = mock(Request.class);
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
