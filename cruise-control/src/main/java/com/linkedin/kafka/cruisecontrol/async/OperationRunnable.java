/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationStep;
import com.linkedin.kafka.cruisecontrol.async.progress.Pending;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An abstract class for the async operation runnables.
 * @param <T>
 */
abstract class OperationRunnable<T> implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(OperationRunnable.class);
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
        // If operation completes successfully (i.e with no exception thrown), record the operation finish time.
        _future.setFinishTimeNs(System.nanoTime());
      }
    } catch (Exception e) {
      LOG.debug("Received exception when trying to execute runnable for \"" + _future.operation() + "\"", e);
      _future.completeExceptionally(e);
    } finally {
      _future.setExecutionThread(null);
      // Clear the interrupt flag after removing itself from the execution thread.
      Thread.interrupted();
    }
  }

  protected abstract T getResult() throws Exception;
}
