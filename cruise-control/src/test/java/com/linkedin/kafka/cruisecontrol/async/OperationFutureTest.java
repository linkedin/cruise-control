/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static org.junit.Assert.*;


public class OperationFutureTest {
  
  @Test
  public void testGetCompleted() throws InterruptedException {
    OperationFuture<Integer> future = new OperationFuture<>("testGetCompleted");
    TestThread t = new TestThread(future::get);
    t.start();
    future.complete(10);
    t.join();
    assertTrue(future.isDone());
    assertEquals(10, t.result());
  }
  
  @Test
  public void testGetCompletedWithTimeout() throws InterruptedException {
    OperationFuture<Integer> future = new OperationFuture<>("testGetCompletedWithTimeout");
    TestThread t = new TestThread(() -> future.get(1, TimeUnit.MILLISECONDS));
    t.start();
    t.join();
    assertFalse(future.isDone());
    assertEquals(0, t.result());
    assertTrue(t.exception() instanceof TimeoutException);
  }
  
  @Test
  public void testGetFailed() throws InterruptedException {
    OperationFuture<Integer> future = new OperationFuture<>("testGetFailed");
    TestThread t = new TestThread(future::get);
    t.start();
    Exception cause = new Exception();
    future.completeExceptionally(cause);
    assertTrue(future.isDone());
    t.join();
    assertEquals(0, t.result());
    assertTrue(t.exception() instanceof ExecutionException);
    assertEquals(cause, t.exception().getCause());
  }
  
  @Test
  public void testCancelInProgressFuture() throws InterruptedException, ExecutionException {
    OperationFuture<Integer> future = new OperationFuture<>("testCancelInProgressFuture");
    AtomicBoolean interrupted = new AtomicBoolean(false);
    
    // An execution thread that should be interrupted before completing the future.
    Thread executionThread = new Thread(new OperationRunnable<Integer>(null, future) {
      @Override
      protected Integer getResult() throws Exception {
        try {
          synchronized (this) {
            while (!interrupted.get()) {
              this.wait();
            }
          }
          return 100;
        } catch (InterruptedException ie) {
          interrupted.set(true);
          throw ie;
        }
      }
    });
    executionThread.start();
    
    TestThread t = new TestThread(future::get);
    t.start();
    future.cancel(true);
    t.join();
    executionThread.join();
    assertEquals(0, t.result());
    assertTrue(t.exception() instanceof CancellationException);
    assertTrue(future.isDone());
    assertTrue(future.isCancelled());
    assertTrue(interrupted.get());
  }
  
  @Test
  public void testCancelPendingFuture() throws InterruptedException, ExecutionException {
    OperationFuture<Integer> future = new OperationFuture<>("testCancelPendingFuture");
    future.cancel(true);
    try {
      future.get();
    } catch (CancellationException ee) {
      // let it go.
    }
  }
  
  @Test
  public void testSetExecutionThread() {
    OperationFuture<Integer> future = new OperationFuture<>("testSetExecutionThread");
    assertTrue(future.setExecutionThread(new Thread()));
    future.cancel(true);
    assertTrue("Should be able to set the execution thread of canceled future to null", 
               future.setExecutionThread(null));
    assertFalse("Should failed to set execution thread for the canceled future.",
                future.setExecutionThread(new Thread()));
  }
  
  @FunctionalInterface
  private interface IntSupplier {
    Integer get() throws TimeoutException, InterruptedException, ExecutionException;
  }
  private static class TestThread extends Thread {
    private final IntSupplier _supplier;
    private int _result = 0;
    private Exception _exception = null;
    
    private TestThread(IntSupplier supplier) {
      _supplier = supplier;
    }
    
    @Override
    public void run() {
      try {
        _result = _supplier.get();
      } catch (Exception e) {
        _exception = e;
        // let it go.
      }
    }
    
    private int result() {
      return _result;
    }
    
    private Exception exception() {
      return _exception;
    }
  }
}
