/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;

/**
 * This class defines a HTTP Basic authenticator with a file based {@link HashLoginService} and uses the default
 * Cruise Control role structure.
 */
public class BasicSecurityProvider extends DefaultRoleSecurityProvider {

  private String _userCredentialsFile;

  @Override
  public void init(KafkaCruiseControlConfig config) {
    super.init(config);
    this._userCredentialsFile = config.getString(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG);
  }

  @Override
  public LoginService loginService() {
    return new HashLoginService("DefaultLoginService", _userCredentialsFile);
  }

  @Override
  public Authenticator authenticator() {
    return new BasicAuthenticator();
  }
}
