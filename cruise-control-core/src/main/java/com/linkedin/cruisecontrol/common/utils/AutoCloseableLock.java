/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.common.utils;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;


public class AutoCloseableLock implements AutoCloseable {

  private final Lock _lock;
  private final AtomicBoolean _closed;

  public AutoCloseableLock(Lock lock) {
    _lock = Utils.validateNotNull(lock, "Lock cannot be null");
    _closed = new AtomicBoolean(false);
    _lock.lock();
  }

  @Override
  public void close() {
    if (_closed.compareAndSet(false, true)) {
      try {
        _lock.unlock();
      } catch (Exception e) {
        throw new IllegalStateException("while invoking close action", e);
      }
    }
  }

  // Visible for testing purpose
  boolean isClosed() {
    return _closed.get();
  }
}
