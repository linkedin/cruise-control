/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import com.linkedin.kafka.cruisecontrol.servlet.security.DummyLoginService;
import com.linkedin.kafka.cruisecontrol.servlet.security.RoleProvider;
import com.linkedin.kafka.cruisecontrol.servlet.security.UserStoreRoleProvider;
import org.apache.kafka.common.security.kerberos.KerberosName;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.security.RoleDelegateUserIdentity;
import org.eclipse.jetty.security.SPNEGOLoginService;
import org.eclipse.jetty.security.SPNEGOUserPrincipal;
import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Session;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.security.auth.Subject;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.function.Function;

/**
 * This class is purely needed to manage the {@link RoleProvider}.
 * For instance if the RoleProvider holds a {@link org.eclipse.jetty.security.PropertyUserStore} then it would load
 * users from the store during the {@link PropertyUserStore#start()} method.
 *
 * @see UserStoreRoleProvider
 */
public class SpnegoLoginServiceWithAuthServiceLifecycle extends ContainerLifeCycle implements LoginService {

  private static final Logger LOG = LoggerFactory.getLogger(SpnegoLoginServiceWithAuthServiceLifecycle.class);
  private static final String GSS_HOLDER_CLASS_NAME = "org.eclipse.jetty.security.SPNEGOLoginService$GSSContextHolder";
  private static final String REQUEST_ATTR_ADDED_NEW_SESSION = SpnegoLoginServiceWithAuthServiceLifecycle.class.getName() + "#ADDED_NEW_SESSION";
  private final GSSManager _gssManager = GSSManager.getInstance();
  private final SPNEGOLoginService _spnegoLoginService;
  private final RoleProvider _roleProvider;
  private final KerberosShortNamer _kerberosShortNamer;
  private Subject _spnegoSubject;
  private GSSCredential _spnegoServiceCredential;
  private Constructor<?> _holderConstructor;

  public SpnegoLoginServiceWithAuthServiceLifecycle(String realm, RoleProvider roleProvider, List<String> principalToLocalRules) {
    _spnegoLoginService = new SPNEGOLoginService(realm, new DummyLoginService());
    _roleProvider = roleProvider;
    _kerberosShortNamer = principalToLocalRules == null || principalToLocalRules.isEmpty()
            ? null
            : KerberosShortNamer.fromUnparsedRules(realm, principalToLocalRules);
  }

  @Override
  protected void doStart() throws Exception {
    addBean(_spnegoLoginService);
    addBean(_roleProvider);
    super.doStart();
    extractSpnegoContext();
  }

  @Override
  public String getName() {
    return _spnegoLoginService.getName();
  }

  @Override
  public UserIdentity login(String username, Object credentials, Request request, Function<Boolean, Session> getOrCreateSession) {
    // save GSS context
    GSSContext gssContext = addContext(request);

    // authentication
    UserIdentity userIdentity = _spnegoLoginService.login(username, credentials, request, getOrCreateSession);
    if (userIdentity == null) {
      return null;
    }
    if (userIdentity instanceof RoleDelegateUserIdentity) {
      return userIdentity;
    }

    // get full principal and create user principal shortname
    String fullPrincipal = getFullPrincipalFromGssContext(gssContext);
    cleanRequest(request);
    String userShortname = getSpnegoUserPrincipalShortname(fullPrincipal);

    SPNEGOUserPrincipal orig;
    if (userIdentity.getUserPrincipal() instanceof SPNEGOUserPrincipal) {
      orig = (SPNEGOUserPrincipal) userIdentity.getUserPrincipal();
    } else {
      orig = null;
    }

    SPNEGOUserPrincipal finalPrincipal = new SPNEGOUserPrincipal(userShortname, orig != null ? orig.getEncodedToken() : null);

    LOG.debug("User {} logged in with full principal {}", finalPrincipal.getName(), fullPrincipal);

    // do authorization and create UserIdentity
    String[] roles = _roleProvider.rolesFor(request, userShortname);
    if (roles == null) {
      return new RoleDelegateUserIdentity(userIdentity.getSubject(), finalPrincipal, null);
    }
    IdentityService is = getIdentityService();
    UserIdentity delegate = is.newUserIdentity(userIdentity.getSubject(), finalPrincipal, roles);
    return new RoleDelegateUserIdentity(userIdentity.getSubject(), finalPrincipal, delegate);
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

  protected String getFullPrincipalFromGssContext(GSSContext gssContext) {
    try {
      return gssContext.getSrcName().toString();
    } catch (GSSException e) {
      throw new RuntimeException("Failed to extract full principal", e);
    }
  }

  // Visible for testing
  void extractSpnegoContext() {
    // All the following code depends on the structure of org.eclipse.jetty.security.SPNEGOLoginService$GSSContextHolder and
    // org.eclipse.jetty.security.SPNEGOLoginService$SpnegoContext
    // If jetty is upgraded, this code might brake
    try {
      Field contextField = SPNEGOLoginService.class.getDeclaredField("_context");
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

  protected GSSContext addContext(Request request) {
    // This tries to inject an externally created GSSContext into SPNEGOLoginService.
    // SPNEGOLoginService drops the realm part of the client principal, but we need that to evaluate the auth to local rules
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

  private void cleanRequest(Request request) {
    if (!"true".equals(request.getAttribute(REQUEST_ATTR_ADDED_NEW_SESSION))) {
      return;
    }
    request.removeAttribute(REQUEST_ATTR_ADDED_NEW_SESSION);
    Session session = request.getSession(false);
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
