/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The future to support async operations in KafkaCruiseControl 
 * @param <T> the return type.
 */
public class OperationFuture<T> extends CompletableFuture<T> {
  private static final Logger LOG = LoggerFactory.getLogger(OperationFuture.class); 
  // The url encoded request url
  private final String _operation;
  private final OperationProgress _operationProgress;
  private volatile Thread _executionThread = null;
  
  public OperationFuture(String operation) {
    _operation = "'" + operation + "'";
    _operationProgress = new OperationProgress();
  }
  
  @Override
  public synchronized boolean cancel(boolean mayInterruptIfRunning) {
    boolean canceled = false;
    if (_executionThread != null) {
      if (mayInterruptIfRunning) {
        canceled = super.cancel(true);
        _executionThread.interrupt();
      }
    } else {
      canceled = super.cancel(true);
    }
    notifyAll();
    return canceled;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    try {
      return super.get();
    } catch (Throwable t) {
      try {
        Field f = Throwable.class.getDeclaredField("detailMessage");
        f.setAccessible(true);
        f.set(t, String.format("Operation '%s' received exception. ", _operation) 
            + (t.getMessage() == null ? "" : t.getMessage()));
      } catch (IllegalAccessException | NoSuchFieldException e) {
        // let it go
      }
      throw t;
    }
  }

  /**
   * Set the execution thread to allow cancellation. The set will be successful unless the future is canceled 
   * and a non-null thread is set to be execution thread.
   * 
   * @param t the thread working for this future.
   * @return true if the execution thread is set successfully, false otherwise.
   */
  public synchronized boolean setExecutionThread(Thread t) {
    // The setting only fails if a thread tries to pick up a canceled operation.
    if (isCancelled() && t != null) {
      return false;
    } else {
      _executionThread = t;
      return true;
    }
  }

  /**
   * @return the string describing the progress of the operation.
   */
  public String progressString() {
    return _operationProgress.toString();
  }

  /**
   * @return the {@link OperationProgress} of this operation.
   */
  public OperationProgress operationProgress() {
     return _operationProgress;
  }
}
