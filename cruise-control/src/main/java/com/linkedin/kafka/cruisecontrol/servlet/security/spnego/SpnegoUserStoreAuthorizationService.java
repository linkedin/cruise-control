/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import com.linkedin.kafka.cruisecontrol.servlet.security.UserStoreAuthorizationService;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.server.UserIdentity;
import javax.servlet.http.HttpServletRequest;

public class SpnegoUserStoreAuthorizationService extends UserStoreAuthorizationService {

  public SpnegoUserStoreAuthorizationService(String privilegesFilePath) {
    super(privilegesFilePath);
  }

  public SpnegoUserStoreAuthorizationService(UserStore userStore) {
    super(userStore);
  }

  @Override
  public UserIdentity getUserIdentity(HttpServletRequest request, String name) {
    int hostSeparator = name.indexOf('/');
    String shortName = hostSeparator > 0 ? name.substring(0, hostSeparator) : name;
    int realmSeparator = shortName.indexOf('@');
    shortName = realmSeparator > 0 ? shortName.substring(0, realmSeparator) : shortName;
    return super.getUserIdentity(request, shortName);
  }
}
