/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.cruisecontrol.common.utils.AutoCloseableLock;
import com.linkedin.cruisecontrol.common.utils.Utils;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.common.KafkaFuture;


class KafkaFutureResultsIterator<T> implements Iterator<KafkaFutureResultsIterator.FutureResult<T>> {

  static class FutureResult<T> {
    private final T _result;
    private final Exception _exception;
    private final KafkaFuture<T> _finishedKafkaFuture;

    FutureResult(T result, Exception exception, KafkaFuture<T> finishedKafkaFuture) {
      if (exception != null && result != null) {
        throw new IllegalArgumentException("Cannot have both result and exception. "
            + "Got result " + result + " and exception " + exception);
      }
      _finishedKafkaFuture = Utils.validateNotNull(finishedKafkaFuture, "Finished Kafka future cannot be null");
      _result = result;
      _exception = exception;
    }

    boolean exceptional() {
      return _exception != null;
    }
    Exception exception() {
      return _exception;
    }

    T result() {
      return _result;
    }
    KafkaFuture<T> finishedKafkaFuture() {
      return _finishedKafkaFuture;
    }
  }

  private static final Duration DEFAULT_FUTURE_CHECK_INTERVAL = Duration.ofMillis(20);
  private final Set<KafkaFuture<T>> _waitingKafkaFutures;
  private final BlockingQueue<FutureResult<T>> _finishedKafkaFutures;
  private final Lock _lock;
  private final ScheduledExecutorService _scheduledExecutorService;

  KafkaFutureResultsIterator(Set<KafkaFuture<T>> waitingKafkaFutures) {
    this(waitingKafkaFutures, DEFAULT_FUTURE_CHECK_INTERVAL);
  }

  KafkaFutureResultsIterator(Set<KafkaFuture<T>> waitingKafkaFutures, Duration futureCheckInterval) {
    if (waitingKafkaFutures == null) {
      throw new IllegalArgumentException("Kafka futures cannot be null");
    }
    if (waitingKafkaFutures.isEmpty()) {
      throw new IllegalArgumentException("Kafka futures cannot be empty");
    }
    Utils.validateNotNull(futureCheckInterval, "Future checking interval cannot be null");
    _waitingKafkaFutures = new HashSet<>(waitingKafkaFutures);
    _lock = new ReentrantLock();
    _scheduledExecutorService = Executors.newScheduledThreadPool(1);
    _finishedKafkaFutures = new ArrayBlockingQueue<>(_waitingKafkaFutures.size());
    _scheduledExecutorService.scheduleAtFixedRate(this::checkFutureStatus, 0, futureCheckInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean hasNext() {
    try (AutoCloseableLock autoCloseableLock = new AutoCloseableLock(_lock)) {
      return _waitingKafkaFutures.size() + _finishedKafkaFutures.size() > 0;
    }
  }

  @Override
  public FutureResult<T> next() {
    if (!hasNext()) {
      throw new IllegalStateException("All Kafka future(s) have finished");
    }
    try {
      return _finishedKafkaFutures.take();
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  private void checkFutureStatus() {
    try (AutoCloseableLock autoCloseableLock = new AutoCloseableLock(_lock)) {
      Iterator<KafkaFuture<T>> waitingFuturesIterator = _waitingKafkaFutures.iterator();
      System.out.println("Put thread starts");
      while (waitingFuturesIterator.hasNext()) {
        KafkaFuture<T> kafkaFuture = waitingFuturesIterator.next();
        if (!kafkaFuture.isDone()) {
          continue;
        }
        waitingFuturesIterator.remove(); // Remove the finished Kafka future from the waiting pool
        T result = null;
        Exception exception = null;
        try {
          result = kafkaFuture.get();
        } catch (Exception e) {
          exception = e;
        }
        enqueueResultOrException(result, exception, kafkaFuture);
      }
      if (_waitingKafkaFutures.isEmpty()) {
        _scheduledExecutorService.shutdown();
      }
    }
  }

  private void enqueueResultOrException(T result, Exception exception, KafkaFuture<T> finishedKafkaFuture) {
    FutureResult<T> resultOrException;
    if (exception != null) {
      resultOrException = new FutureResult<>(null, exception, finishedKafkaFuture);
    } else {
      resultOrException = new FutureResult<>(result, null, finishedKafkaFuture);
    }
    try {
      _finishedKafkaFutures.put(resultOrException);
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  // Visible for testing purpose
  boolean isShutdown() {
    return _scheduledExecutorService.isShutdown();
  }
}
