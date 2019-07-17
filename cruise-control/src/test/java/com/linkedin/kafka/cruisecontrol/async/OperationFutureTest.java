/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.servlet.response.PauseSamplingResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.ResumeSamplingResult;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


public class OperationFutureTest {
  private static CruiseControlResponse DEFAULT_RESULT = new PauseSamplingResult(null);

  @Test
  public void testGetCompleted() throws InterruptedException {
    OperationFuture future = new OperationFuture("testGetCompleted");
    TestThread t = new TestThread(future);
    t.start();
    CruiseControlResponse expectedResponse = new ResumeSamplingResult(null);
    future.complete(expectedResponse);
    t.join();
    assertTrue(future.isDone());
    assertEquals(expectedResponse, t.result());
  }

  @Test
  public void testGetCompletedWithTimeout() throws InterruptedException {
    OperationFuture future = new OperationFuture("testGetCompletedWithTimeout");
    TestThread t = new TestThread(future, 1, TimeUnit.MILLISECONDS);
    t.start();
    t.join();
    assertFalse(future.isDone());
    assertEquals(DEFAULT_RESULT, t.result());
    assertTrue(t.exception() instanceof TimeoutException);
  }

  @Test
  public void testGetFailed() throws InterruptedException {
    OperationFuture future = new OperationFuture("testGetFailed");
    TestThread t = new TestThread(future);
    t.start();
    Exception cause = new Exception();
    future.completeExceptionally(cause);
    assertTrue(future.isDone());
    t.join();
    assertEquals(DEFAULT_RESULT, t.result());
    assertTrue(t.exception() instanceof ExecutionException);
    assertEquals(cause, t.exception().getCause());
  }

  @Test
  public void testCancelInProgressFuture() throws InterruptedException {
    OperationFuture future = new OperationFuture("testCancelInProgressFuture");
    AtomicBoolean interrupted = new AtomicBoolean(false);

    Semaphore resultIsCalledSemaphore = new Semaphore(1);
    resultIsCalledSemaphore.acquire();
    // An execution thread that should be interrupted before completing the future.
    Thread executionThread = new Thread(new OperationRunnable(null, future) {
      @Override
      protected CruiseControlResponse getResult() throws Exception {
        resultIsCalledSemaphore.release();
        try {
          synchronized (this) {
            while (!interrupted.get()) {
              this.wait();
            }
          }
          return new ResumeSamplingResult(null);
        } catch (InterruptedException ie) {
          interrupted.set(true);
          throw ie;
        }
      }
    });
    executionThread.start();

    TestThread t = new TestThread(future);
    t.start();
    resultIsCalledSemaphore.acquire();
    future.cancel(true);
    t.join();
    executionThread.join();
    assertTrue(t.result() instanceof PauseSamplingResult);
    assertTrue(t.exception() instanceof CancellationException);
    assertTrue(future.isDone());
    assertTrue(future.isCancelled());
    assertTrue(interrupted.get());
  }

  @Test
  public void testCancelPendingFuture() throws InterruptedException, ExecutionException {
    OperationFuture future = new OperationFuture("testCancelPendingFuture");
    future.cancel(true);
    try {
      future.get();
    } catch (CancellationException ee) {
      // let it go.
    }
  }

  @Test
  public void testSetExecutionThread() {
    OperationFuture future = new OperationFuture("testSetExecutionThread");
    assertTrue(future.setExecutionThread(new Thread()));
    future.cancel(true);
    assertTrue("Should be able to set the execution thread of canceled future to null",
               future.setExecutionThread(null));
    assertFalse("Should failed to set execution thread for the canceled future.",
                future.setExecutionThread(new Thread()));
  }

  private static class TestThread extends Thread {
    private final OperationFuture _future;
    private CruiseControlResponse _result = DEFAULT_RESULT;
    private Exception _exception = null;
    private final long _timeout;
    private final TimeUnit _unit;

    private TestThread(OperationFuture future) {
      this(future, -1, null);
    }

    private TestThread(OperationFuture future, long timeout, TimeUnit unit) {
      _future = future;
      _timeout = timeout;
      _unit = unit;
    }

    @Override
    public void run() {
      try {
        _result = _unit != null ? _future.get(_timeout, _unit) : _future.get();
      } catch (Exception e) {
        _exception = e;
        // let it go.
      }
    }

    private CruiseControlResponse result() {
      return _result;
    }

    private Exception exception() {
      return _exception;
    }
  }
}
