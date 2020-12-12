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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.common.KafkaFuture;
import org.eclipse.jetty.util.BlockingArrayQueue;


class KafkaFuturesWatcher<T> implements Iterator<KafkaFuturesWatcher.ExceptionOrResult<T>> {

  static class ExceptionOrResult<T> {
    private final T _result;
    private final Exception _exception;

    private ExceptionOrResult(T result, Exception exception) {
      if (exception != null && result != null) {
        throw new IllegalArgumentException("Cannot have both result and exception. "
            + "Got result " + result + " and exception " + exception);
      }
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
  }

  private static final Duration DEFAULT_FUTURE_CHECK_INTERVAL = Duration.ofMillis(20);
  private final Set<KafkaFuture<T>> _waitingKafkaFutures;
  private final BlockingQueue<ExceptionOrResult<T>> _finishedKafkaFutures;
  private final Lock _lock;
  private final ScheduledExecutorService _scheduledExecutorService;

  KafkaFuturesWatcher(Set<KafkaFuture<T>> waitingKafkaFutures) {
    this(waitingKafkaFutures, DEFAULT_FUTURE_CHECK_INTERVAL);
  }

  KafkaFuturesWatcher(Set<KafkaFuture<T>> waitingKafkaFutures, Duration futureCheckInterval) {
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
    _finishedKafkaFutures = new BlockingArrayQueue<>(_waitingKafkaFutures.size());
    _scheduledExecutorService.scheduleAtFixedRate(this::checkFutureStatus, 0, futureCheckInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean hasNext() {
    try (AutoCloseableLock autoCloseableLock = new AutoCloseableLock(_lock)) {
      return _waitingKafkaFutures.size() + _finishedKafkaFutures.size() > 0;
    }
  }

  @Override
  public ExceptionOrResult<T> next() {
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
      Iterator<KafkaFuture<T>> iterator = _waitingKafkaFutures.iterator();

      while (iterator.hasNext()) {
        KafkaFuture<T> kafkaFuture = iterator.next();
        if (!kafkaFuture.isDone()) {
          continue;
        }
        iterator.remove(); // Remove the finished Kafka future from the waiting pool
        T result = null;
        Exception exception = null;
        try {
          result = kafkaFuture.get();
        } catch (Exception e) {
          exception = e;
        }
        enqueueResultOrException(result, exception);
      }
      if (_waitingKafkaFutures.isEmpty()) {
        _scheduledExecutorService.shutdown();
      }
    }
  }

  private void enqueueResultOrException(T result, Exception exception) {
    ExceptionOrResult<T> resultOrException;
    if (exception != null) {
      resultOrException = new ExceptionOrResult<>(null, exception);
    } else {
      resultOrException = new ExceptionOrResult<>(result, null);
    }
    try {
      _finishedKafkaFutures.put(resultOrException);
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }
}
