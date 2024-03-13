/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.security.DefaultRoleSecurityProvider;
import org.apache.kafka.common.security.kerberos.KerberosName;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.AuthorizationService;
import org.eclipse.jetty.security.authentication.ConfigurableSpnegoAuthenticator;
import java.nio.file.Paths;
import java.util.List;

/**
 * Defines an SPNEGO capable login service using the HTTP Negotiate authentication mechanism.
 */
public class SpnegoSecurityProvider extends DefaultRoleSecurityProvider {

  protected String _privilegesFilePath;
  protected String _keyTabPath;
  protected KerberosName _spnegoPrincipal;
  private List<String> _spnegoPrincipalToLocalRules;

  @Override
  public void init(KafkaCruiseControlConfig config) {
    super.init(config);
    _privilegesFilePath = config.getString(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG);
    _keyTabPath = config.getString(WebServerConfig.SPNEGO_KEYTAB_FILE_CONFIG);
    _spnegoPrincipal = KerberosName.parse(config.getString(WebServerConfig.SPNEGO_PRINCIPAL_CONFIG));
    _spnegoPrincipalToLocalRules = config.getList(WebServerConfig.SPNEGO_PRINCIPAL_TO_LOCAL_RULES_CONFIG);
  }

  @Override
  public LoginService loginService() {
    SpnegoLoginServiceWithAuthServiceLifecycle loginService = new SpnegoLoginServiceWithAuthServiceLifecycle(
            _spnegoPrincipal.realm(), authorizationService(), _spnegoPrincipalToLocalRules);
    loginService.setServiceName(_spnegoPrincipal.serviceName());
    loginService.setHostName(_spnegoPrincipal.hostName());
    loginService.setKeyTabPath(Paths.get(_keyTabPath));
    return loginService;
  }

  @Override
  public Authenticator authenticator() {
    return new ConfigurableSpnegoAuthenticator();
  }

  public AuthorizationService authorizationService() {
    return new SpnegoUserStoreAuthorizationService(_privilegesFilePath);
  }
}
