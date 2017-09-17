/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.exception;

/**
 * The exception indicates that the monitor have not collected sufficient snapshots for the cluster, yet.
 */
public class NotEnoughSnapshotsException extends CruiseControlException {
  public NotEnoughSnapshotsException(String msg) {
    super(msg);
  }
}
