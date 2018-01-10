/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The future to support async operations in KafkaCruiseControl 
 * @param <T> the return type.
 */
public class OperationFuture<T> implements Future<T> {
  // The url encoded request url
  private final String _operation;
  private final OperationProgress _operationProgress;
  private volatile T _result = null;
  private volatile Exception _exception = null;
  private volatile Thread _executionThread = null;
  private volatile boolean _canceled = false;
  
  public OperationFuture(String operation) {
    _operation = operation;
    _operationProgress = new OperationProgress();
  }
  
  @Override
  public synchronized boolean cancel(boolean mayInterruptIfRunning) {
    if (_executionThread != null) {
      if (mayInterruptIfRunning) {
        _canceled = true;
        _executionThread.interrupt();
      }
    } else {
      _canceled = true;
    }
    notifyAll();
    return _canceled;
  }

  @Override
  public synchronized boolean isCancelled() {
    return _canceled;
  }

  @Override
  public synchronized boolean isDone() {
    return _result != null || _exception != null || _canceled;
  }

  @Override
  public synchronized T get() throws InterruptedException, ExecutionException {
    if (_canceled) {
      throw new ExecutionException(new CancellationException("The operation + " + _operation + " has been canceled"));
    }
    if (_result != null) {
      return _result;
    } else if (_exception != null) {
      throw new ExecutionException("Encountered exception during execution of operation " + _operation, _exception);
    }
    while (!isDone()) {
      this.wait();
    }
    if (_canceled) {
      throw new ExecutionException(new CancellationException("The operation + " + _operation + " has been canceled"));
    }
    if (_result != null) {
      return _result;
    } else if (_exception != null) {
      throw new ExecutionException("Encountered exception during execution of operation " + _operation, _exception);
    } else {
      throw new IllegalStateException("The thread is waken up but the future is not done");
    }
  }

  @Override
  public synchronized T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    if (_canceled) {
      throw new ExecutionException(new CancellationException("The operation + " + _operation + " has been canceled"));
    }
    if (_result != null) {
      return _result;
    } else if (_exception != null) {
      throw new ExecutionException("Encountered exception during execution of operation " + _operation, _exception);
    }
    long now = System.currentTimeMillis();
    long deadLine = now < Long.MAX_VALUE - timeout ? now + timeout : Long.MAX_VALUE;
    while (!isDone() && now < deadLine) {
      this.wait(deadLine - now);
      now = System.currentTimeMillis();
    }
    if (_canceled) {
      throw new ExecutionException(new CancellationException("The operation " + _operation + " has been canceled"));
    }
    if (_result != null) {
      return _result;
    } else if (_exception != null) {
      throw new ExecutionException("Encountered exception during execution of operation " + _operation, _exception);
    } else {
      throw new TimeoutException("Did not get the result before timeout of " + timeout + " " + unit);
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
    if (_canceled && t != null) {
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

  /**
   * Fail the future with the given exception.
   * @param e the exception caused the future to fail.
   */
  public synchronized void fail(Exception e) {
    if (e == null) {
      throw new IllegalArgumentException("The exception cannot be null");
    }
    if (_canceled) {
      return;
    }
    if (_exception != null) {
      IllegalStateException exception =
          new IllegalStateException("Cannot fail a failed future. operation: " + _operation 
                                        + ". New exception is in the suppressed exception. Existing exception: ", 
                                    _exception);
      exception.addSuppressed(e);
      throw exception;
    } else if (_result != null) {
      throw new IllegalStateException(
          "Cannot fail a succeeded future. operation: " + _operation + ". Existing result " + _result);
    }

    _exception = e;
    this.notifyAll();
  }

  /**
   * Complete the future with a result.
   * @param result the result of this future.
   */
  public synchronized void complete(T result) {
    if (result == null) {
      throw new IllegalArgumentException("The result cannot be null");
    }
    if (_canceled) {
      return;
    }
    if (_exception != null) {
      throw new IllegalStateException("Cannot complete a failed future. Operation: " + _operation + ". Existing exception: ", _exception);
    } else if (_result != null) {
      throw new IllegalStateException(
          "Cannot complete a succeeded future. Operation: " + _operation + ". Existing result " + _result);
    }
    _result = result;
    this.notifyAll();
  }
}
