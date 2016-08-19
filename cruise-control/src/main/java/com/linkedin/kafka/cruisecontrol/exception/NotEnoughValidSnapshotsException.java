/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.exception;

/**
 * This exception indicates that the percentage of partitions modeled in the load monitor is not enough.
 */
public class NotEnoughValidSnapshotsException extends KafkaCruiseControlException {
  public NotEnoughValidSnapshotsException(String msg) {
    super(msg);
  }
}
