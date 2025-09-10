/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import com.linkedin.kafka.cruisecontrol.servlet.security.DefaultRoleSecurityProvider;
import com.linkedin.kafka.cruisecontrol.servlet.security.RoleProvider;
import com.linkedin.kafka.cruisecontrol.servlet.security.SecurityUtils;
import org.eclipse.jetty.security.RolePrincipal;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import java.util.List;
import java.util.regex.Pattern;

public class TrustedProxyUserStoreRoleProvider extends AbstractLifeCycle implements RoleProvider {
  private final UserStore _serviceUserStore;
  private final Pattern _trustedProxyIpPattern;

  public TrustedProxyUserStoreRoleProvider(List<String> userNames, String trustedProxyIpPattern) {
    _serviceUserStore = new UserStore();
    userNames.forEach(u ->
        _serviceUserStore.addUser(u, SecurityUtils.NO_CREDENTIAL,
            new String[]{DefaultRoleSecurityProvider.ADMIN}));
    this._trustedProxyIpPattern = trustedProxyIpPattern == null ? null : Pattern.compile(trustedProxyIpPattern);
  }

  @Override
  public String[] rolesFor(Request request, String name) {
    int nameHostSeparatorIndex = name.indexOf('/');
    String serviceName = (nameHostSeparatorIndex > 0) ? name.substring(0, nameHostSeparatorIndex) : name;

    List<RolePrincipal> rolePrincipals = _serviceUserStore.getRolePrincipals(serviceName);
    if (rolePrincipals == null || rolePrincipals.isEmpty()) {
      return null;
    }
    String[] roles = rolePrincipals.stream().map(RolePrincipal::getName).toArray(String[]::new);

    if (_trustedProxyIpPattern == null) {
      return roles;
    }

    String remoteAddr = Request.getRemoteAddr(request);
    return _trustedProxyIpPattern.matcher(remoteAddr).matches() ? roles : null;
  }

  @Override
  protected void doStart() throws Exception {
    _serviceUserStore.start();
    super.doStart();
  }

  @Override
  protected void doStop() throws Exception {
    try {
      _serviceUserStore.stop();
    } finally {
      super.doStop();
    }
  }
}
