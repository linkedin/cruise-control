/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.cruisecontrol.common.utils;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AutoCloseableLockTest {

  @Test
  public void testIdempotentClose() {
    Lock lock = new ReentrantLock();
    AutoCloseableLock autoCloseableLock = new AutoCloseableLock(lock);
    assertFalse(autoCloseableLock.isClosed());
    autoCloseableLock.close();
    assertTrue(autoCloseableLock.isClosed());

    // Calling the close method again should be a no-op
    autoCloseableLock.close();
    assertTrue(autoCloseableLock.isClosed());
  }
}
