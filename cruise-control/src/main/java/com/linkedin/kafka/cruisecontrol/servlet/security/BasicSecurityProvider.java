/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.util.security.Constraint;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BasicSecurityProvider implements SecurityProvider {

  public static final String ADMIN = "ADMIN";
  public static final String USER = "USER";
  public static final String VIEWER = "VIEWER";

  private final String _userCredentialsFile;
  private final String _webServerApiUrlPrefix;

  public BasicSecurityProvider(KafkaCruiseControlConfig config) {
    this._webServerApiUrlPrefix = config.getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG);
    this._userCredentialsFile = config.getString(WebServerConfig.BASIC_AUTH_CREDENTIALS_FILE_CONFIG);
  }

  @Override
  public List<ConstraintMapping> constraintMappings() {
    List<ConstraintMapping> constraintMappings = new ArrayList<>();
    CruiseControlEndPoint.getEndpoints().forEach(ep -> constraintMappings.add(mapping(ep, USER, ADMIN)));
    CruiseControlEndPoint.postEndpoints().forEach(ep -> constraintMappings.add(mapping(ep, ADMIN)));
    return constraintMappings;
  }

  @Override
  public LoginService loginService() {
    return new HashLoginService("DefaultLoginService", _userCredentialsFile);
  }

  @Override
  public Authenticator authenticator() {
    return new BasicAuthenticator();
  }

  @Override
  public Set<String> roles() {
    Set<String> roles = new HashSet<>();
    roles.add(ADMIN);
    roles.add(USER);
    roles.add(VIEWER);
    return roles;
  }

  private ConstraintMapping mapping(CruiseControlEndPoint endpoint, String... roles) {
    Constraint constraint = new Constraint();
    constraint.setName(Constraint.__BASIC_AUTH);
    constraint.setRoles(roles);
    constraint.setAuthenticate(true);
    ConstraintMapping mapping = new ConstraintMapping();
    mapping.setPathSpec(_webServerApiUrlPrefix.replace("*", endpoint.name().toLowerCase()));
    mapping.setConstraint(constraint);
    return mapping;
  }
}
