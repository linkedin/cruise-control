/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import com.linkedin.kafka.cruisecontrol.servlet.security.spnego.SpnegoLoginServiceWithAuthServiceLifecycle;
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
  public static final boolean READ_ONLY_SUBJECT = true;
  // authorizes the end user that is passed in via the doAs header
  private final AuthorizationService _endUserAuthorizer;
  // use encapsulation instead of inheritance as it's easier to test
  private final SpnegoLoginServiceWithAuthServiceLifecycle _delegateSpnegoLoginService;
  private final SpnegoLoginServiceWithAuthServiceLifecycle _fallbackSpnegoLoginService;
  // we can fall back to spnego and authenticate the service user only if no doAs user provided
  private final boolean _fallbackToSpnegoAllowed;

  /**
   * Creates a new instance based on the kerberos realm, the list of trusted proxies, their allowed IP pattern and the
   * {@link AuthorizationService} that stores the end users.
   * @param realm is the kerberos realm of the spnego service principal that is used by Cruise Control
   * @param userAuthorizer authorizes the user that is passed in via the doAs header
   * @param trustedProxies is a list of kerberos service shortnames that identifies the trusted proxies
   * @param trustedProxyIpPattern is a Java regex pattern that defines which IP addresses can be accepted by
   *                              Cruise Control as trusted proxies
   */
  public TrustedProxyLoginService(String realm, AuthorizationService userAuthorizer, List<String> trustedProxies,
                                  String trustedProxyIpPattern, boolean fallbackToSpnegoAllowed) {
    _delegateSpnegoLoginService = new SpnegoLoginServiceWithAuthServiceLifecycle(
        realm, new TrustedProxyAuthorizationService(trustedProxies, trustedProxyIpPattern));
    _fallbackSpnegoLoginService = new SpnegoLoginServiceWithAuthServiceLifecycle(realm, userAuthorizer);
    _endUserAuthorizer = userAuthorizer;
    _fallbackToSpnegoAllowed = fallbackToSpnegoAllowed;
  }

  // visible for testing
  TrustedProxyLoginService(SpnegoLoginServiceWithAuthServiceLifecycle delegateSpnegoLoginService,
                           SpnegoLoginServiceWithAuthServiceLifecycle fallbackSpnegoLoginService,
                           AuthorizationService userAuthorizer, boolean fallbackToSpnegoAllowed) {
    _delegateSpnegoLoginService = delegateSpnegoLoginService;
    _fallbackSpnegoLoginService = fallbackSpnegoLoginService;
    _endUserAuthorizer = userAuthorizer;
    _fallbackToSpnegoAllowed = fallbackToSpnegoAllowed;
  }

  // ------- ConfigurableSpnegoLoginService methods -------

  /**
   * Sets the service name for spnego login.
   * @param serviceName the service name for spnego login
   */
  public void setServiceName(String serviceName) {
    _delegateSpnegoLoginService.setServiceName(serviceName);
    _fallbackSpnegoLoginService.setServiceName(serviceName);
  }

  /**
   * Sets the hostname for spnego login.
   * @param hostName hostname for spnego login.
   */
  public void setHostName(String hostName) {
    _delegateSpnegoLoginService.setHostName(hostName);
    _fallbackSpnegoLoginService.setHostName(hostName);
  }

  /**
   * Sets the keytab path for spnego login.
   * @param path keytab path for spnego login
   */
  public void setKeyTabPath(Path path) {
    _delegateSpnegoLoginService.setKeyTabPath(path);
    _fallbackSpnegoLoginService.setKeyTabPath(path);
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
    String doAsUser = request.getParameter(DO_AS);
    if (doAsUser == null && _fallbackToSpnegoAllowed) {
      SpnegoUserIdentity fallbackIdentity = (SpnegoUserIdentity) _fallbackSpnegoLoginService.login(username, credentials, request);
      SpnegoUserPrincipal fallbackPrincipal = (SpnegoUserPrincipal) fallbackIdentity.getUserPrincipal();
      if (!fallbackIdentity.isEstablished()) {
        LOG.info("Service user {} isn't authorized as spnego fallback principal", fallbackPrincipal.getName());
      }
      return fallbackIdentity;
    } else {
      SpnegoUserIdentity serviceIdentity = (SpnegoUserIdentity) _delegateSpnegoLoginService.login(username, credentials, request);
      SpnegoUserPrincipal servicePrincipal = (SpnegoUserPrincipal) serviceIdentity.getUserPrincipal();
      LOG.info("Authorizing proxy user {} from {} service", doAsUser, servicePrincipal.getName());
      UserIdentity doAsIdentity = null;
      if (doAsUser != null && !doAsUser.isEmpty()) {
        doAsIdentity = _endUserAuthorizer.getUserIdentity((HttpServletRequest) request, doAsUser);
      }

      Principal principal = new TrustedProxyPrincipal(doAsUser, servicePrincipal);
      Subject subject = new Subject(READ_ONLY_SUBJECT, Collections.singleton(principal), Collections.emptySet(), Collections.emptySet());

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
    if (_endUserAuthorizer instanceof LifeCycle) {
      ((LifeCycle) _endUserAuthorizer).start();
    }
    _delegateSpnegoLoginService.start();
    _fallbackSpnegoLoginService.start();
    super.doStart();
  }

  @Override
  protected void doStop() throws Exception {
    super.doStop();
    _fallbackSpnegoLoginService.start();
    _delegateSpnegoLoginService.stop();
    if (_endUserAuthorizer instanceof LifeCycle) {
      ((LifeCycle) _endUserAuthorizer).stop();
    }
  }
}
