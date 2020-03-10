/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import com.linkedin.kafka.cruisecontrol.servlet.security.UserStoreAuthorizationService;
import org.eclipse.jetty.security.ConfigurableSpnegoLoginService;
import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.security.authentication.AuthorizationService;
import org.eclipse.jetty.util.component.LifeCycle;

/**
 * This class is purely needed in order to manage the {@link AuthorizationService} if it is a {@link LifeCycle} bean.
 * For instance if the AuthorizationService holds a {@link org.eclipse.jetty.security.PropertyUserStore} then it would load
 * users from the store during the {@link PropertyUserStore#start()} method.
 *
 * @see UserStoreAuthorizationService
 */
public class SpnegoLoginServiceWithAuthServiceLifecycle extends ConfigurableSpnegoLoginService {

  private final AuthorizationService _authorizationService;

  public SpnegoLoginServiceWithAuthServiceLifecycle(String realm, AuthorizationService authorizationService) {
    super(realm, authorizationService);
    _authorizationService = authorizationService;
  }

  @Override
  protected void doStart() throws Exception {
    if (_authorizationService instanceof LifeCycle) {
      ((LifeCycle) _authorizationService).start();
    }
    super.doStart();
  }

  @Override
  protected void doStop() throws Exception {
    super.doStop();
    if (_authorizationService instanceof LifeCycle) {
      ((LifeCycle) _authorizationService).stop();
    }
  }
}
