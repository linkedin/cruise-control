/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import com.linkedin.kafka.cruisecontrol.servlet.ExposedPropertyUserStore;
import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.resource.PathResourceFactory;
import org.eclipse.jetty.util.resource.Resource;
import java.nio.file.Path;

/**
 * Can be used for authorization scenarios where a file can be created in a secure location with a relatively
 * low number of users. It follows the <code>username: password [,rolename ...]</code> format which corresponds to
 * the format used with {@link org.eclipse.jetty.security.HashLoginService}.
 */
public class UserStoreAuthorizationService extends AbstractLifeCycle implements AuthorizationService {

  private final ExposedPropertyUserStore _userStore;

  public UserStoreAuthorizationService(String privilegesFilePath) {
    this(userStoreFromFile(privilegesFilePath));
  }

  public UserStoreAuthorizationService(ExposedPropertyUserStore userStore) {
    _userStore = userStore;
  }

  @Override
  public UserIdentity getUserIdentity(Request request, String name) {
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

  private static ExposedPropertyUserStore userStoreFromFile(String privilegesFilePath) {
    ExposedPropertyUserStore userStore = new ExposedPropertyUserStore();
    Resource res = new PathResourceFactory().newResource(Path.of(privilegesFilePath));
    userStore.setConfig(res);
    return userStore;
  }
}
