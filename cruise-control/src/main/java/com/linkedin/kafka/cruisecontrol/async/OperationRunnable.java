/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationStep;
import com.linkedin.kafka.cruisecontrol.async.progress.Pending;
import java.util.List;


/**
 * An abstract class for the async operation runnables.
 * @param <T>
 */
abstract class OperationRunnable<T> implements Runnable {
  protected final KafkaCruiseControl _kafkaCruiseControl;
  protected final OperationFuture<T> _future;
  
  OperationRunnable(KafkaCruiseControl kafkaCruiseControl, OperationFuture<T> future) {
    _kafkaCruiseControl = kafkaCruiseControl;
    _future = future;
  }

  /**
   * @return the corresponding {@link OperationFuture} for the runnable.
   */
  protected OperationFuture<T> future() {
    return _future;
  }

  @Override
  public void run() {
    try {
      // Only execute the runnable if the future is not canceled.
      if (_future.setExecutionThread(Thread.currentThread())) {
        List<OperationStep> steps = _future.operationProgress().progress();
        if (!steps.isEmpty() && steps.get(0) instanceof Pending) {
          ((Pending) steps.get(0)).done();
        }
        _future.complete(getResult());
      }
    } catch (Exception e) {
      _future.completeExceptionally(e);
    } finally {
      _future.setExecutionThread(null);
      // Clear the interrupt flag after removing itself from the execution thread.
      Thread.interrupted();
    }
  }
  
  protected abstract T getResult() throws Exception;
}
