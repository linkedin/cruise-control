/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import com.linkedin.kafka.cruisecontrol.servlet.security.spnego.SpnegoLoginServiceWithAuthServiceLifecycle;
import org.eclipse.jetty.security.ConfigurableSpnegoLoginService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.SpnegoUserIdentity;
import org.eclipse.jetty.security.SpnegoUserPrincipal;
import org.eclipse.jetty.security.authentication.AuthorizationService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import java.nio.file.Path;
import java.security.Principal;
import java.util.Collections;
import java.util.List;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DO_AS;

/**
 * <code>TrustedProxyLoginService</code> is a special SPNEGO login service where we only allow a list of trusted services
 * to act on behalf of clients. The login service authenticates the trusted party but creates credentials for the client
 * based on the {@link AuthorizationService}.
 */
public class TrustedProxyLoginService extends ContainerLifeCycle implements LoginService {

  private static final Logger LOG = LoggerFactory.getLogger(TrustedProxyLoginService.class);
  private final AuthorizationService _trustedUserAuthorizer;
  // use encapsulation instead of inheritance as it's easier to test
  private final ConfigurableSpnegoLoginService _delegateSpnegoLoginService;

  /**
   * Creates a new instance based on the kerberos realm, the list of trusted proxies, their allowed IP pattern and the
   * {@link AuthorizationService} that stores the end users.
   * @param realm
   * @param userAuthorizer
   * @param trustedProxies
   * @param trustedProxyIpPattern
   */
  public TrustedProxyLoginService(String realm, AuthorizationService userAuthorizer, List<String> trustedProxies, String trustedProxyIpPattern) {
    _delegateSpnegoLoginService = new SpnegoLoginServiceWithAuthServiceLifecycle(
        realm, new TrustedProxyAuthorizationService(trustedProxies, trustedProxyIpPattern));
    _trustedUserAuthorizer = userAuthorizer;
  }

  // visible for testing
  TrustedProxyLoginService(ConfigurableSpnegoLoginService delegateSpnegoLoginService, AuthorizationService trustedProxyAuthorizer) {
    _delegateSpnegoLoginService = delegateSpnegoLoginService;
    _trustedUserAuthorizer = trustedProxyAuthorizer;
  }

  // ------- ConfigurableSpnegoLoginService methods -------

  public void setServiceName(String serviceName) {
    _delegateSpnegoLoginService.setServiceName(serviceName);
  }

  public void setHostName(String hostName) {
    _delegateSpnegoLoginService.setHostName(hostName);
  }

  public void setKeyTabPath(Path path) {
    _delegateSpnegoLoginService.setKeyTabPath(path);
  }

  // ------- LoginService methods -------

  @Override
  public String getName() {
    return _delegateSpnegoLoginService.getName();
  }

  @Override
  public UserIdentity login(String username, Object credentials, ServletRequest request) {
    if (!(request instanceof HttpServletRequest)) {
      return null;
    }
    SpnegoUserIdentity serviceIdentity = (SpnegoUserIdentity) _delegateSpnegoLoginService.login(username, credentials, request);
    SpnegoUserPrincipal servicePrincipal = (SpnegoUserPrincipal) serviceIdentity.getUserPrincipal();
    String doAsUser = request.getParameter(DO_AS);
    LOG.info("Authorizing proxy user {} from {} service", doAsUser, servicePrincipal.getName());
    UserIdentity doAsIdentity = null;
    if (doAsUser != null && !doAsUser.isEmpty()) {
      doAsIdentity = _trustedUserAuthorizer.getUserIdentity((HttpServletRequest) request, doAsUser);
    }

    Principal principal = new TrustedProxyPrincipal(doAsUser, servicePrincipal);
    Subject subject = new Subject(true, Collections.singleton(principal), Collections.emptySet(), Collections.emptySet());

    if (!serviceIdentity.isEstablished()) {
      LOG.info("Service user {} isn't authorized as a trusted proxy", servicePrincipal.getName());
      return new SpnegoUserIdentity(subject, principal, null);
    } else {
      if (doAsIdentity == null) {
        LOG.info("Couldn't authorize user {}", doAsUser);
      }
      return new SpnegoUserIdentity(subject, principal, doAsIdentity);
    }

  }

  @Override
  public boolean validate(UserIdentity user) {
    return _delegateSpnegoLoginService.validate(user);
  }

  @Override
  public IdentityService getIdentityService() {
    return _delegateSpnegoLoginService.getIdentityService();
  }

  @Override
  public void setIdentityService(IdentityService service) {
    _delegateSpnegoLoginService.setIdentityService(service);
  }

  @Override
  public void logout(UserIdentity user) {
    _delegateSpnegoLoginService.logout(user);
  }

  // ------- ContainerLifeCycle methods -------

  @Override
  protected void doStart() throws Exception {
    if (_trustedUserAuthorizer instanceof LifeCycle) {
      ((LifeCycle) _trustedUserAuthorizer).start();
    }
    _delegateSpnegoLoginService.start();
    super.doStart();
  }

  @Override
  protected void doStop() throws Exception {
    super.doStop();
    _delegateSpnegoLoginService.stop();
    if (_trustedUserAuthorizer instanceof LifeCycle) {
      ((LifeCycle) _trustedUserAuthorizer).stop();
    }
  }
}
