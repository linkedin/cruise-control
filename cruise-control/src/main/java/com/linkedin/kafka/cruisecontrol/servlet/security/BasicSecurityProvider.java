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
import java.util.List;

public class BasicSecurityProvider implements SecurityProvider {

  private String _userCredentialsFile;
  private String _webServerApiUrlPrefix;

  @Override
  public void init(KafkaCruiseControlConfig config) {
    this._webServerApiUrlPrefix = config.getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG);
    this._userCredentialsFile = config.getString(WebServerConfig.BASIC_AUTH_CREDENTIALS_FILE_CONFIG);
  }

  @Override
  public List<ConstraintMapping> constraintMappings() {
    List<ConstraintMapping> constraintMappings = new ArrayList<>();
    CruiseControlEndPoint.getEndpoints().forEach(ep -> {
      if (ep == CruiseControlEndPoint.BOOTSTRAP || ep == CruiseControlEndPoint.TRAIN) {
        constraintMappings.add(mapping(ep, ADMIN));
      } else {
        constraintMappings.add(mapping(ep, USER, ADMIN));
      }
    });
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
