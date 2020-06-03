/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import com.linkedin.kafka.cruisecontrol.servlet.security.DefaultRoleSecurityProvider;
import com.linkedin.kafka.cruisecontrol.servlet.security.SecurityUtils;
import com.linkedin.kafka.cruisecontrol.servlet.security.UserStoreAuthorizationService;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.server.UserIdentity;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SpnegoUserStoreAuthorizationServiceTest {

  private static final String TEST_USER = "testUser";

  @Test
  public void testPrincipalNames() {
    UserStore users = new UserStore();
    users.addUser(TEST_USER, SecurityUtils.NO_CREDENTIAL, new String[] { DefaultRoleSecurityProvider.ADMIN });
    UserStoreAuthorizationService usas = new SpnegoUserStoreAuthorizationService(users);

    UserIdentity result = usas.getUserIdentity(null, TEST_USER + "/host@REALM");
    assertNotNull(result);
    assertEquals(TEST_USER, result.getUserPrincipal().getName());

    result = usas.getUserIdentity(null, TEST_USER + "@REALM");
    assertNotNull(result);
    assertEquals(TEST_USER, result.getUserPrincipal().getName());

    result = usas.getUserIdentity(null, TEST_USER + "/host");
    assertNotNull(result);
    assertEquals(TEST_USER, result.getUserPrincipal().getName());

    result = usas.getUserIdentity(null, TEST_USER);
    assertNotNull(result);
    assertEquals(TEST_USER, result.getUserPrincipal().getName());
  }
}
