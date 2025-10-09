/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import com.linkedin.kafka.cruisecontrol.servlet.security.RoleProvider;
import com.linkedin.kafka.cruisecontrol.servlet.security.spnego.SpnegoLoginServiceWithAuthServiceLifecycle;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.RoleDelegateUserIdentity;
import org.eclipse.jetty.security.SPNEGOUserPrincipal;
import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Session;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.security.auth.Subject;
import java.nio.file.Path;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DO_AS;

/**
 * {@code TrustedProxyLoginService} is a special SPNEGO login service where we only allow a list of trusted services
 * to act on behalf of clients.
 */
public class TrustedProxyLoginService extends ContainerLifeCycle implements LoginService {

  private static final Logger LOG = LoggerFactory.getLogger(TrustedProxyLoginService.class);
  public static final boolean READ_ONLY_SUBJECT = true;
  // authorizes the end user passed in via the doAs header
  private final RoleProvider _roleProvider;
  // use encapsulation instead of inheritance as it's easier to test
  private final SpnegoLoginServiceWithAuthServiceLifecycle _delegateSpnegoLoginService;
  private final SpnegoLoginServiceWithAuthServiceLifecycle _fallbackSpnegoLoginService;
  // we can fall back to spnego and authenticate the service user only if no doAs user provided
  private final boolean _fallbackToSpnegoAllowed;

  /**
   * Creates a new instance based on the kerberos realm, the list of trusted proxies, their allowed IP pattern and the
   * {@link RoleProvider} that stores the end users.
   * @param realm is the kerberos realm of the spnego service principal that is used by Cruise Control
   * @param roleProvider authorizes the user passed in via the doAs header
   * @param trustedProxies is a list of kerberos service shortnames that identifies the trusted proxies
   * @param trustedProxyIpPattern is a Java regex pattern that defines which IP addresses can be accepted by
   *                              Cruise Control as trusted proxies
   */
  public TrustedProxyLoginService(String realm, RoleProvider roleProvider, List<String> trustedProxies,
                                  String trustedProxyIpPattern, boolean fallbackToSpnegoAllowed, List<String> principalToLocalRules) {
    _delegateSpnegoLoginService = new SpnegoLoginServiceWithAuthServiceLifecycle(
        realm, new TrustedProxyUserStoreRoleProvider(trustedProxies, trustedProxyIpPattern), principalToLocalRules);
    _fallbackSpnegoLoginService = new SpnegoLoginServiceWithAuthServiceLifecycle(realm, roleProvider, principalToLocalRules);
    _roleProvider = roleProvider;
    _fallbackToSpnegoAllowed = fallbackToSpnegoAllowed;
  }

  // visible for testing
  TrustedProxyLoginService(SpnegoLoginServiceWithAuthServiceLifecycle delegateSpnegoLoginService,
                           SpnegoLoginServiceWithAuthServiceLifecycle fallbackSpnegoLoginService,
                           RoleProvider roleProvider, boolean fallbackToSpnegoAllowed) {
    _delegateSpnegoLoginService = delegateSpnegoLoginService;
    _fallbackSpnegoLoginService = fallbackSpnegoLoginService;
    _roleProvider = roleProvider;
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
  public UserIdentity login(String username, Object credentials, Request request, Function<Boolean, Session> getOrCreateSession) {
      Fields reqParameters;
      try {
          reqParameters = Request.getParameters(request);
      } catch (Exception e) {
          throw new RuntimeException(e);
      }
      String doAsUser = reqParameters.getValue(DO_AS);
    if (doAsUser == null && _fallbackToSpnegoAllowed) {
      RoleDelegateUserIdentity fallbackIdentity = (RoleDelegateUserIdentity) _fallbackSpnegoLoginService.login(username,
          credentials, request, getOrCreateSession);
      if (!fallbackIdentity.isEstablished()) {
        SPNEGOUserPrincipal fallbackPrincipal = (SPNEGOUserPrincipal) fallbackIdentity.getUserPrincipal();
        LOG.info("Service user {} isn't authorized as spnego fallback principal", fallbackPrincipal.getName());
      }
      return fallbackIdentity;
    } else {
      RoleDelegateUserIdentity serviceIdentity = (RoleDelegateUserIdentity) _delegateSpnegoLoginService.login(username,
          credentials, request, getOrCreateSession);
      SPNEGOUserPrincipal servicePrincipal = (SPNEGOUserPrincipal) serviceIdentity.getUserPrincipal();
      LOG.info("Authorizing proxy user {} from {} service", doAsUser, servicePrincipal.getName());
      UserIdentity doAsIdentity = null;
      if (doAsUser != null && !doAsUser.isEmpty()) {
        String[] roles = _roleProvider.rolesFor(request, doAsUser);
        doAsIdentity = getIdentityService().newUserIdentity(serviceIdentity.getSubject(), servicePrincipal, roles);
      }

      Principal principal = new TrustedProxyPrincipal(doAsUser, servicePrincipal);
      Subject subject = new Subject(READ_ONLY_SUBJECT, Collections.singleton(principal), Collections.emptySet(), Collections.emptySet());

      if (!serviceIdentity.isEstablished()) {
        LOG.info("Service user {} isn't authorized as a trusted proxy", servicePrincipal.getName());
        return new RoleDelegateUserIdentity(subject, principal, null);
      } else {
        if (doAsIdentity == null) {
          LOG.info("Couldn't authorize user {}", doAsUser);
        }
        return new RoleDelegateUserIdentity(subject, principal, doAsIdentity);
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
    if (_roleProvider instanceof LifeCycle) {
      ((LifeCycle) _roleProvider).start();
    }
    _delegateSpnegoLoginService.start();
    _fallbackSpnegoLoginService.start();
    super.doStart();
  }

  @Override
  protected void doStop() throws Exception {
    super.doStop();
    _fallbackSpnegoLoginService.stop();
    _delegateSpnegoLoginService.stop();
    if (_roleProvider instanceof LifeCycle) {
      ((LifeCycle) _roleProvider).stop();
    }
  }
}
