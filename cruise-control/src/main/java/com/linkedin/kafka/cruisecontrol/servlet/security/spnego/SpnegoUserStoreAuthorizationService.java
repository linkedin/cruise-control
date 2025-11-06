/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import com.linkedin.kafka.cruisecontrol.servlet.ExposedPropertyUserStore;
import com.linkedin.kafka.cruisecontrol.servlet.security.UserStoreAuthorizationService;
import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.server.Request;

public class SpnegoUserStoreAuthorizationService extends UserStoreAuthorizationService {

  public SpnegoUserStoreAuthorizationService(String privilegesFilePath) {
    super(privilegesFilePath);
  }

  public SpnegoUserStoreAuthorizationService(ExposedPropertyUserStore userStore) {
    super(userStore);
  }

  @Override
  public UserIdentity getUserIdentity(Request request, String name) {
    int hostSeparator = name.indexOf('/');
    String shortName = hostSeparator > 0 ? name.substring(0, hostSeparator) : name;
    int realmSeparator = shortName.indexOf('@');
    shortName = realmSeparator > 0 ? shortName.substring(0, realmSeparator) : shortName;
    return super.getUserIdentity(request, shortName);
  }
}
