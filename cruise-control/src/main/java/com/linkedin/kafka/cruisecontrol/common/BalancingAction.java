/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

public enum BalancingAction {
  REPLICA_MOVEMENT("REPLICA"), LEADERSHIP_MOVEMENT("LEADER");

  private final String _balancingAction;

  BalancingAction(String balancingAction) {
    _balancingAction = balancingAction;
  }

  public String balancingAction() {
    return _balancingAction;
  }

  @Override
  public String toString() {
    return _balancingAction;
  }
}