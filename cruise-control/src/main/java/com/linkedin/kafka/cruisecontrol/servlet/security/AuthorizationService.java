/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.server.Request;

/**
 * An interface to get roles for a given user.
 */
@FunctionalInterface
public interface AuthorizationService {
  /**
   * @param request the current request
   * @param name the user name
   * @return a {@link UserIdentity} to query for roles of the given user
   */
  UserIdentity getUserIdentity(Request request, String name);
}
