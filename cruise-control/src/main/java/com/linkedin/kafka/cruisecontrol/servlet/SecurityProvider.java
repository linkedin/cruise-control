/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.LoginService;

import java.util.List;
import java.util.Set;

public interface SecurityProvider {

  List<ConstraintMapping> getConstraintMappings();

  LoginService getLoginService();

  Authenticator getAuthenticator();

  Set<String> getRoles();
}
