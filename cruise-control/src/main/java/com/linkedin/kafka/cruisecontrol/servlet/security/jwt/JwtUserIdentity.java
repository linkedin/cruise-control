/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.jwt;

import org.eclipse.jetty.server.UserIdentity;

import javax.security.auth.Subject;
import java.security.Principal;

public class JwtUserIdentity implements UserIdentity {

  private Subject _subject;
  private Principal _principal;
  private UserIdentity _roleDelegate;

  JwtUserIdentity(Subject subject, Principal principal, UserIdentity roleDelegate) {
    _subject = subject;
    _principal = principal;
    _roleDelegate = roleDelegate;
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
  public boolean isUserInRole(String role, Scope scope) {
    return _roleDelegate != null && _roleDelegate.isUserInRole(role, scope);
  }
}
