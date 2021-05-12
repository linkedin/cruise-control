/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.security.authentication.AuthorizationService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import javax.servlet.http.HttpServletRequest;

/**
 * Can be used for authorization scenarios where a file can be created in a secure location with a relatively
 * low number of users. It follows the <code>username: password [,rolename ...]</code> format which corresponds to
 * the format used with {@link org.eclipse.jetty.security.HashLoginService}.
 */
public class UserStoreAuthorizationService extends AbstractLifeCycle implements AuthorizationService {

  private final UserStore _userStore;

  public UserStoreAuthorizationService(String privilegesFilePath) {
    this(userStoreFromFile(privilegesFilePath));
  }

  public UserStoreAuthorizationService(UserStore userStore) {
    _userStore = userStore;
  }

  @Override
  public UserIdentity getUserIdentity(HttpServletRequest request, String name) {
    return _userStore.getUserIdentity(name);
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();
    _userStore.start();
  }

  @Override
  protected void doStop() throws Exception {
    _userStore.stop();
    super.doStop();
  }

  private static UserStore userStoreFromFile(String privilegesFilePath) {
    PropertyUserStore userStore = new PropertyUserStore();
    userStore.setConfig(privilegesFilePath);
    return userStore;
  }
}
