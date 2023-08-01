/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import com.linkedin.kafka.cruisecontrol.servlet.security.DummyAuthorizationService;
import com.linkedin.kafka.cruisecontrol.servlet.security.UserStoreAuthorizationService;
import org.apache.kafka.common.security.kerberos.KerberosName;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.eclipse.jetty.security.ConfigurableSpnegoLoginService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.security.SpnegoUserIdentity;
import org.eclipse.jetty.security.SpnegoUserPrincipal;
import org.eclipse.jetty.security.authentication.AuthorizationService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.util.List;

/**
 * This class is purely needed in order to manage the {@link AuthorizationService}.
 * For instance if the AuthorizationService holds a {@link org.eclipse.jetty.security.PropertyUserStore} then it would load
 * users from the store during the {@link PropertyUserStore#start()} method.
 *
 * @see UserStoreAuthorizationService
 */
public class SpnegoLoginServiceWithAuthServiceLifecycle extends ContainerLifeCycle implements LoginService {

  private static final Logger LOG = LoggerFactory.getLogger(SpnegoLoginServiceWithAuthServiceLifecycle.class);
  private static final String GSS_HOLDER_CLASS_NAME = "org.eclipse.jetty.security.ConfigurableSpnegoLoginService$GSSContextHolder";
  private static final String REQUEST_ATTR_ADDED_NEW_SESSION = SpnegoLoginServiceWithAuthServiceLifecycle.class.getName() + "#ADDED_NEW_SESSION";
  private final GSSManager _gssManager = GSSManager.getInstance();
  private final ConfigurableSpnegoLoginService _spnegoLoginService;
  private final AuthorizationService _authorizationService;
  private final KerberosShortNamer _kerberosShortNamer;
  private Subject _spnegoSubject;
  private GSSCredential _spnegoServiceCredential;
  private Constructor<?> _holderConstructor;

  public SpnegoLoginServiceWithAuthServiceLifecycle(String realm, AuthorizationService authorizationService, List<String> principalToLocalRules) {
    _spnegoLoginService = new ConfigurableSpnegoLoginService(realm, new DummyAuthorizationService());
    _authorizationService = authorizationService;
    _kerberosShortNamer = principalToLocalRules == null || principalToLocalRules.isEmpty()
            ? null
            : KerberosShortNamer.fromUnparsedRules(realm, principalToLocalRules);
  }

  @Override
  protected void doStart() throws Exception {
    addBean(_spnegoLoginService);
    addBean(_authorizationService);
    super.doStart();
    extractSpnegoContext();
  }

  @Override
  public String getName() {
    return _spnegoLoginService.getName();
  }

  @Override
  public UserIdentity login(String username, Object credentials, ServletRequest req) {
    // save GSS context
    HttpServletRequest request = (HttpServletRequest) req;
    GSSContext gssContext = addContext(request);

    // authentication
    SpnegoUserIdentity userIdentity = (SpnegoUserIdentity) _spnegoLoginService.login(username, credentials, req);
    SpnegoUserPrincipal userPrincipal = (SpnegoUserPrincipal) userIdentity.getUserPrincipal();

    // get full principal and create user principal shortname
    String fullPrincipal = getFullPrincipalFromGssContext(gssContext);
    cleanRequest(request);
    LOG.debug("User {} logged in with full principal {}", userPrincipal.getName(), fullPrincipal);
    String userShortname = getSpnegoUserPrincipalShortname(fullPrincipal);

    // do authorization and create UserIdentity
    userPrincipal = new SpnegoUserPrincipal(userShortname, userPrincipal.getEncodedToken());
    UserIdentity roleDelegate = _authorizationService.getUserIdentity((HttpServletRequest) req, userShortname);
    return new SpnegoUserIdentity(userIdentity.getSubject(), userPrincipal, roleDelegate);
  }

  @Override
  public boolean validate(UserIdentity user) {
    return _spnegoLoginService.validate(user);
  }

  @Override
  public IdentityService getIdentityService() {
    return _spnegoLoginService.getIdentityService();
  }

  @Override
  public void setIdentityService(IdentityService service) {
    _spnegoLoginService.setIdentityService(service);
  }

  @Override
  public void logout(UserIdentity user) {
    _spnegoLoginService.logout(user);
  }

  public void setServiceName(String serviceName) {
    _spnegoLoginService.setServiceName(serviceName);
  }

  public void setHostName(String hostName) {
    _spnegoLoginService.setHostName(hostName);
  }

  public void setKeyTabPath(Path keyTabFile) {
    _spnegoLoginService.setKeyTabPath(keyTabFile);
  }

  private String getFullPrincipal(HttpServletRequest request) {
    String fullPrincipal;
    try {
      fullPrincipal = ((GSSContext) request.getSession()
              .getAttribute(GSS_HOLDER_CLASS_NAME))
              .getSrcName()
              .toString();
    } catch (GSSException e) {
      throw new RuntimeException(e);
    }
    return fullPrincipal;
  }

  private String getSpnegoUserPrincipalShortname(String fullPrincipal) {
    PrincipalName userPrincipalName = PrincipalValidator.parsePrincipal("", fullPrincipal);

    if (_kerberosShortNamer == null) {
      return userPrincipalName.getPrimary();
    }

    try {
      String userShortname = _kerberosShortNamer.shortName(new KerberosName(userPrincipalName.getPrimary(),
              userPrincipalName.getInstance(), userPrincipalName.getRealm()));
      LOG.debug("Principal {} was shortened to {}", userPrincipalName, userShortname);
      return userShortname;
    } catch (IOException e) {
      throw new RuntimeException("Could not generate short name for principal " + userPrincipalName, e);
    }
  }

  private String getFullPrincipalFromGssContext(GSSContext gssContext) {
    try {
      return gssContext.getSrcName().toString();
    } catch (GSSException e) {
      throw new RuntimeException("Failed to extract full principal", e);
    }
  }

  // Visible for testing
  void extractSpnegoContext() {
    // All the following code depends on the structure of org.eclipse.jetty.security.ConfigurableSpnegoLoginService$GSSContextHolder and
    // org.eclipse.jetty.security.ConfigurableSpnegoLoginService$SpnegoContext
    // If jetty is upgraded, this code might brake
    try {
      Field contextField = ConfigurableSpnegoLoginService.class.getDeclaredField("_context");
      contextField.setAccessible(true);
      Object spnegoContext = contextField.get(_spnegoLoginService);
      Class<?> contextClass = spnegoContext.getClass();
      Field contextSubjectField = contextClass.getDeclaredField("_subject");
      contextSubjectField.setAccessible(true);
      _spnegoSubject = (Subject) contextSubjectField.get(spnegoContext);
      Field contextCredentialField = contextClass.getDeclaredField("_serviceCredential");
      contextCredentialField.setAccessible(true);
      _spnegoServiceCredential = (GSSCredential) contextCredentialField.get(spnegoContext);
      Class<?> gssHolder = Class.forName(GSS_HOLDER_CLASS_NAME);
      _holderConstructor = gssHolder.getDeclaredConstructor(GSSContext.class);
      _holderConstructor.setAccessible(true);
    } catch (NoSuchFieldException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException e) {
      throw new RuntimeException("Failed to init SPNEGO context", e);
    }

  }

  private GSSContext addContext(HttpServletRequest request) {
    // This tries to inject an externally created GSSContext into ConfigurableSpnegoLoginService.
    // ConfigurableSpnegoLoginService drops the realm part of the client principal, but we need that to evaluate the auth to local rules
    // By injecting the GSSContext through the session, we can get access to the full principal
    // If jetty is upgraded, this code might brake
    try {
      GSSContext gssContext = Subject.doAs(_spnegoSubject, newGSSContext(_spnegoServiceCredential));
      Object holder = _holderConstructor.newInstance(gssContext);
      boolean needsNewSession = request.getSession(false) == null;
      if (needsNewSession) {
        request.setAttribute(REQUEST_ATTR_ADDED_NEW_SESSION, "true");
      }
      request.getSession(true).setAttribute(GSS_HOLDER_CLASS_NAME, holder);
      return gssContext;
    } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
      throw new RuntimeException("Failed to perform SPNEGO authentication", e);
    }
  }

  private void cleanRequest(HttpServletRequest request) {
    if (!"true".equals(request.getAttribute(REQUEST_ATTR_ADDED_NEW_SESSION))) {
      return;
    }
    request.removeAttribute(REQUEST_ATTR_ADDED_NEW_SESSION);
    HttpSession session = request.getSession();
    if (session != null) {
      try {
        session.invalidate();
      } catch (Exception ignored) {
        // NOP
      }
    }
  }

  private PrivilegedAction<GSSContext> newGSSContext(GSSCredential serviceCredential) {
    return () -> {
      try {
        return _gssManager.createContext(serviceCredential);
      } catch (GSSException e) {
        throw new RuntimeException(e);
      }
    };
  }

}
