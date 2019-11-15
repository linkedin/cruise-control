/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.LoginService;

import java.util.List;
import java.util.Set;

/**
 * This class delegates to the Jetty implementation of authentication features and is used for setting
 * up Cruise Control security to provide authentication and authorization rules over the WEB API.
 */
public interface SecurityProvider {

  /**
   * Creates or gets a list of constraints that are put on the API endpoints, for instance only
   * allow certain roles on certain endpoints.
   */
  List<ConstraintMapping> constraintMappings();

  /**
   * Associates a username, credentials and roles with a {@link org.eclipse.jetty.server.UserIdentity}
   * that will be used by Jetty to manage the authentication.
   */
  LoginService loginService();

  /**
   * Defines the request authentication method which is responsible to send challenges
   * according to authentication method and decide if the user has valid credentials according
   * to the authentication method.
   */
  Authenticator authenticator();

  /**
   * A list of strings that define the possible roles in Cruise Control.
   */
  Set<String> roles();
}
