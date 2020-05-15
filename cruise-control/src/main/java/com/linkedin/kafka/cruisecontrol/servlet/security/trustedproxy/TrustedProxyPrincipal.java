/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import org.eclipse.jetty.security.SpnegoUserPrincipal;

public class TrustedProxyPrincipal extends SpnegoUserPrincipal {

  private SpnegoUserPrincipal _serviceUserPrincipal;
  private String _doAsPrincipal;

  public TrustedProxyPrincipal(String doAsPrincipal, SpnegoUserPrincipal serviceUserPrincipal) {
    super(doAsPrincipal, serviceUserPrincipal.getEncodedToken());
    _doAsPrincipal = doAsPrincipal;
    _serviceUserPrincipal = serviceUserPrincipal;
  }

  @Override
  public String getName() {
    return _doAsPrincipal;
  }

  public SpnegoUserPrincipal servicePrincipal() {
    return _serviceUserPrincipal;
  }
}
