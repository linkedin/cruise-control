/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import com.linkedin.kafka.cruisecontrol.servlet.security.DefaultRoleSecurityProvider;
import com.linkedin.kafka.cruisecontrol.servlet.security.SecurityUtils;
import com.linkedin.kafka.cruisecontrol.servlet.security.UserStoreRoleProvider;
import org.eclipse.jetty.security.RolePrincipal;
import org.eclipse.jetty.security.UserStore;
import org.junit.Test;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SpnegoUserStoreRoleProviderTest {

  private static final String TEST_USER = "testUser";

  static class CapturingUserStore extends UserStore {
    private volatile String _lastLookup;

    public String lastLookup() {
      return _lastLookup;
    }

    @Override
    public List<RolePrincipal> getRolePrincipals(String userName) {
      _lastLookup = userName;
      return super.getRolePrincipals(userName);
    }
  }

  @Test
  public void testPrincipalNames() {
    CapturingUserStore users = new CapturingUserStore();
    users.addUser(TEST_USER, SecurityUtils.NO_CREDENTIAL, new String[] { DefaultRoleSecurityProvider.ADMIN });
    UserStoreRoleProvider usas = new SpnegoUserStoreRoleProvider(users);

    String[] result = usas.rolesFor(null, TEST_USER + "/host@REALM");
    assertNotNull(result);
    assertEquals("ADMIN", result[0]);
    assertEquals(TEST_USER, users.lastLookup());

    result = usas.rolesFor(null, TEST_USER + "@REALM");
    assertNotNull(result);
    assertEquals("ADMIN", result[0]);
    assertEquals(TEST_USER, users.lastLookup());

    result = usas.rolesFor(null, TEST_USER + "/host");
    assertNotNull(result);
    assertEquals("ADMIN", result[0]);
    assertEquals(TEST_USER, users.lastLookup());

    result = usas.rolesFor(null, TEST_USER);
    assertNotNull(result);
    assertEquals("ADMIN", result[0]);
    assertEquals(TEST_USER, users.lastLookup());
  }
}
