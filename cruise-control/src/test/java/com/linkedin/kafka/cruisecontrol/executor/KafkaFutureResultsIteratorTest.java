/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.common.KafkaFuture;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertFalse;


public class KafkaFutureResultsIteratorTest {

  @Test
  public void testFutureResult() {
    // Case 1: The case in which both result and exception are null should be allowed
    KafkaFuture<Void> mockVoidKafkaFuture = EasyMock.mock(KafkaFuture.class);
    KafkaFutureResultsIterator.FutureResult<Void> futureResult =
        new KafkaFutureResultsIterator.FutureResult<>(null, null, mockVoidKafkaFuture);
    assertNull(futureResult.exception());
    assertNull(futureResult.result());
    assertEquals(mockVoidKafkaFuture, futureResult.finishedKafkaFuture());

    // Case 2: Result is provided without an exception
    KafkaFuture<Integer> mockIntegerKafkaFuture = EasyMock.mock(KafkaFuture.class);
    KafkaFutureResultsIterator.FutureResult<Integer> futureIntegerResult =
        new KafkaFutureResultsIterator.FutureResult<>(1, null, mockIntegerKafkaFuture);
    assertNull(futureIntegerResult.exception());
    assertEquals(futureIntegerResult.result(), Integer.valueOf(1));
    assertEquals(mockIntegerKafkaFuture, futureIntegerResult.finishedKafkaFuture());

    // Case 3: Exception is provided and no result
    Exception expectedException = new IllegalStateException("This exception is used for testing purpose");
    futureIntegerResult = new KafkaFutureResultsIterator.FutureResult<>(null, expectedException, mockIntegerKafkaFuture);
    assertEquals(futureIntegerResult.exception(), expectedException);
    assertNull(futureIntegerResult.result());
    assertEquals(mockIntegerKafkaFuture, futureIntegerResult.finishedKafkaFuture());

    // Case 4: Should not provide both a exception and a result together
    assertThrows(IllegalArgumentException.class, () -> new KafkaFutureResultsIterator.FutureResult<>(1, expectedException, mockIntegerKafkaFuture));
  }

  @Test
  public void testKafkaFutureResultsIterator() throws ExecutionException, InterruptedException {
    final int mockFutureCount = 3;
    List<KafkaFuture<Integer>> mockKafkaFutures = new ArrayList<>(mockFutureCount);
    for (int i = 0; i < mockFutureCount; i++) {
      mockKafkaFutures.add(EasyMock.mock(KafkaFuture.class));
    }

    // Case 1: 3 futures and they all finish and return successfully on the first round of checking
    for (int i = 0; i < mockFutureCount; i++) {
      EasyMock.expect(mockKafkaFutures.get(i).isDone()).andReturn(true).once();
      EasyMock.expect(mockKafkaFutures.get(i).get()).andReturn(i).once();
    }
    mockKafkaFutures.forEach(EasyMock::replay);

    KafkaFutureResultsIterator<Integer> futureResultsIterator = new KafkaFutureResultsIterator<>(new HashSet<>(mockKafkaFutures));
    Set<KafkaFutureResultsIterator.FutureResult<Integer>> futureResults = collectResults(futureResultsIterator);
    assertEquals(new HashSet<>(Arrays.asList(0, 1, 2)),
                 futureResults.stream().map(KafkaFutureResultsIterator.FutureResult::result).collect(Collectors.toSet()));

    assertTrue(futureResultsIterator.isShutdown());
    verifyAndResetMocks(mockKafkaFutures);

    // Case 2: 3 futures and they all finish and return successfully on the third round of checking
    // First 2 rounds of checking, futures all return NOT done
    mockKafkaFutures.forEach(mockKafkaFuture -> {
      EasyMock.expect(mockKafkaFuture.isDone()).andReturn(false).times(2);
    });

    // Third round of checking return done
    mockKafkaFutures.forEach(mockKafkaFuture -> {
      EasyMock.expect(mockKafkaFuture.isDone()).andReturn(true);
    });
    for (int i = 0; i < mockFutureCount; i++) {
      EasyMock.expect(mockKafkaFutures.get(i).get()).andReturn(i).once();
    }
    mockKafkaFutures.forEach(EasyMock::replay);

    futureResultsIterator = new KafkaFutureResultsIterator<>(new HashSet<>(mockKafkaFutures));
    futureResults = collectResults(futureResultsIterator);
    assertEquals(new HashSet<>(Arrays.asList(0, 1, 2)),
                 futureResults.stream().map(KafkaFutureResultsIterator.FutureResult::result).collect(Collectors.toSet()));

    assertTrue(futureResultsIterator.isShutdown());
    verifyAndResetMocks(mockKafkaFutures);

    // Case 3: 3 futures and one of them throws exception and other 2 returns results on the first round of checking
    mockKafkaFutures.forEach(mockKafkaFuture -> {
      EasyMock.expect(mockKafkaFuture.isDone()).andReturn(true);
    });
    Exception expectedException = new IllegalStateException("An expected exception to simulate how future throws exception");
    EasyMock.expect(mockKafkaFutures.get(0).get()).andReturn(0).once();
    EasyMock.expect(mockKafkaFutures.get(1).get()).andThrow(expectedException).once();
    EasyMock.expect(mockKafkaFutures.get(2).get()).andReturn(2).once();
    mockKafkaFutures.forEach(EasyMock::replay);

    futureResultsIterator = new KafkaFutureResultsIterator<>(new HashSet<>(mockKafkaFutures));
    futureResults = collectResults(futureResultsIterator);

    Set<Integer> results = new HashSet<>();
    Exception gotException = null;

    for (KafkaFutureResultsIterator.FutureResult<Integer> futureResult : futureResults) {
      if (futureResult.exceptional()) {
        assertNull("Only expect one exception", gotException);
        gotException = futureResult.exception();
      } else {
        results.add(futureResult.result());
      }
    }

    assertEquals(new HashSet<>(Arrays.asList(0, 2)), results);
    assertEquals(expectedException, gotException);
    assertTrue(futureResultsIterator.isShutdown());
    verifyAndResetMocks(mockKafkaFutures);

    // Case 4: 3 futures and all of them throws exception on the first round of checking
    mockKafkaFutures.forEach(mockKafkaFuture -> {
      EasyMock.expect(mockKafkaFuture.isDone()).andReturn(true);
    });
    Set<Exception> expectedExceptions = new HashSet<>(mockFutureCount);
    for (int i = 0; i < mockFutureCount; i++) {
      Exception exception = new IllegalStateException("An expected exception to simulate how future throws exception " + i);
      expectedExceptions.add(exception);
      EasyMock.expect(mockKafkaFutures.get(i).get()).andThrow(exception);
    }
    mockKafkaFutures.forEach(EasyMock::replay);

    futureResultsIterator = new KafkaFutureResultsIterator<>(new HashSet<>(mockKafkaFutures));
    futureResults = collectResults(futureResultsIterator);

    assertEquals(expectedExceptions,
                 futureResults.stream().map(KafkaFutureResultsIterator.FutureResult::exception).collect(Collectors.toSet()));
    assertTrue(futureResultsIterator.isShutdown());
    verifyAndResetMocks(mockKafkaFutures);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyInputKafkaFutureResultsIterator() {
    new KafkaFutureResultsIterator<>(Collections.emptySet());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullInputKafkaFutureResultsIterator() {
    new KafkaFutureResultsIterator<>(null);
  }

  @Test(expected = IllegalStateException.class)
  public void testIllegalUseOfKafkaFutureResultsIterator() throws ExecutionException, InterruptedException {
    // Call next() when the iterator does not have next element
    KafkaFuture<Void> mockKafkaFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockKafkaFuture.isDone()).andReturn(true).once();
    EasyMock.expect(mockKafkaFuture.get()).andReturn(null).once();
    EasyMock.replay(mockKafkaFuture);

    KafkaFutureResultsIterator<Void> futureResultsIterator = new KafkaFutureResultsIterator<>(Collections.singleton(mockKafkaFuture));
    Set<KafkaFutureResultsIterator.FutureResult<Void>> results = collectResults(futureResultsIterator);
    assertEquals(results.size(), 1);
    for (KafkaFutureResultsIterator.FutureResult<Void> result : results) {
      assertFalse(result.exceptional());
      assertNull(result.exception());
      assertNull(result.result()); // When the future is of type Void, the result is null
    }
    assertTrue(futureResultsIterator.isShutdown());
    EasyMock.verify(mockKafkaFuture);

    // This call triggers the exception
    futureResultsIterator.next();
  }

  private <T> Set<KafkaFutureResultsIterator.FutureResult<T>> collectResults(KafkaFutureResultsIterator<T> futureResultsIterator) {
    Set<KafkaFutureResultsIterator.FutureResult<T>> futureResults = new HashSet<>();
    while (futureResultsIterator.hasNext()) {
      futureResults.add(futureResultsIterator.next());
    }
    return futureResults;
  }

  private void verifyAndResetMocks(List<KafkaFuture<Integer>> mockKafkaFutures) {
    mockKafkaFutures.forEach(EasyMock::verify);
    mockKafkaFutures.forEach(EasyMock::reset);
  }
}
