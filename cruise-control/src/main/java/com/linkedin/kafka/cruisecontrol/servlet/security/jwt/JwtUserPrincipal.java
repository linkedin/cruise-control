/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.jwt;

import java.security.Principal;

public class JwtUserPrincipal implements Principal {

  private final String _username;
  private final String _serializedToken;

  JwtUserPrincipal(String username, String serializedToken) {
    _username = username;
    _serializedToken = serializedToken;
  }

  @Override
  public String getName() {
    return _username;
  }

  public String getSerializedToken() {
    return _serializedToken;
  }
}
