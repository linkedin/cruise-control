/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.common.utils;

import java.util.concurrent.locks.Lock;


public class AutoCloseableLock implements AutoCloseable {

  private final Lock _lock;

  public AutoCloseableLock(Lock lock) {
    _lock = Utils.validateNotNull(lock, "Lock cannot be null");
    _lock.lock();
  }

  @Override
  public void close() {
    try {
      _lock.unlock();
    } catch (Exception e) {
      throw new IllegalStateException("while invoking close action", e);
    }
  }
}
