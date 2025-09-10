/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.jwt;

import org.eclipse.jetty.security.UserIdentity;
import javax.security.auth.Subject;
import java.security.Principal;
import java.util.Arrays;

public class JwtUserIdentity implements UserIdentity {

  private final Subject _subject;
  private final Principal _principal;
  private final String[] _roles;

  JwtUserIdentity(Subject subject, Principal principal, String[] roles) {
    _subject = subject;
    _principal = principal;
    _roles = roles;
  }

  @Override
  public Subject getSubject() {
    return _subject;
  }

  @Override
  public Principal getUserPrincipal() {
    return _principal;
  }

  @Override
  public boolean isUserInRole(String role) {
    return _roles != null && Arrays.asList(_roles).contains(role);
  }
}
