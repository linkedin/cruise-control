/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencySummary;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExecutionConcurrencySummaryTest {
  private final Map<Integer, Integer> _interBrokerConcurrencyMap = Map.of(0, 5, 1, 6, 2, 7);
  private final Map<Integer, Integer> _intraBrokerConcurrencyMap = Map.of(0, 10, 1, 11, 2, 12);
  private final Map<Integer, Integer> _leadershipBrokerConcurrencyMap = Map.of(0, 100, 1, 200, 2, 300);

  @Test
  public void testInterBrokerMinMaxAvgConcurrency() {
    ExecutionConcurrencySummary summary = new ExecutionConcurrencySummary(
        true, _interBrokerConcurrencyMap, _intraBrokerConcurrencyMap, _leadershipBrokerConcurrencyMap);

    assertEquals(summary.getMaxExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA), 7);
    assertEquals(summary.getMinExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA), 5);
    assertEquals(summary.getAvgExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA), 6, 0.01);
  }

  @Test
  public void testIntraBrokerMinMaxAvgConcurrency() {
    ExecutionConcurrencySummary summary = new ExecutionConcurrencySummary(
        true, _interBrokerConcurrencyMap, _intraBrokerConcurrencyMap, _leadershipBrokerConcurrencyMap);

    assertEquals(summary.getMaxExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA), 12);
    assertEquals(summary.getMinExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA), 10);
    assertEquals(summary.getAvgExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA), 11, 0.01);
  }

  @Test
  public void testLeadershipMinMaxAvgConcurrency() {
    ExecutionConcurrencySummary summary = new ExecutionConcurrencySummary(
        true, _interBrokerConcurrencyMap, _intraBrokerConcurrencyMap, _leadershipBrokerConcurrencyMap);

    assertEquals(summary.getMaxExecutionConcurrency(ConcurrencyType.LEADERSHIP), 300);
    assertEquals(summary.getMinExecutionConcurrency(ConcurrencyType.LEADERSHIP), 100);
    assertEquals(summary.getAvgExecutionConcurrency(ConcurrencyType.LEADERSHIP), 200, 0.01);
  }

  @Test
  public void testInvalidConcurrencySummaryShouldReturnZeroForMinMaxAvg() {
    ExecutionConcurrencySummary summary = new ExecutionConcurrencySummary(
        false, _interBrokerConcurrencyMap, _intraBrokerConcurrencyMap, _leadershipBrokerConcurrencyMap);

    assertEquals(summary.getMaxExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA), 0);
    assertEquals(summary.getMinExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA), 0);
    assertEquals(summary.getAvgExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA), 0, 0.01);
    assertEquals(summary.getMaxExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA), 0);
    assertEquals(summary.getMinExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA), 0);
    assertEquals(summary.getAvgExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA), 0, 0.01);
    assertEquals(summary.getMaxExecutionConcurrency(ConcurrencyType.LEADERSHIP), 0);
    assertEquals(summary.getMinExecutionConcurrency(ConcurrencyType.LEADERSHIP), 0);
    assertEquals(summary.getAvgExecutionConcurrency(ConcurrencyType.LEADERSHIP), 0, 0.01);
  }

  @Test
  public void testShouldThrowExceptionIfConcurrencyMapNotFullyPopulated() {
    ExecutionConcurrencySummary summary = new ExecutionConcurrencySummary(
        true, Collections.emptyMap(), _intraBrokerConcurrencyMap, _leadershipBrokerConcurrencyMap);
    assertThrows(IllegalArgumentException.class, () -> summary.getMaxExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    assertThrows(IllegalArgumentException.class, () -> summary.getMinExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    assertThrows(IllegalArgumentException.class, () -> summary.getAvgExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    assertThrows(IllegalArgumentException.class, () -> summary.getMaxExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    assertThrows(IllegalArgumentException.class, () -> summary.getMinExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    assertThrows(IllegalArgumentException.class, () -> summary.getAvgExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    assertThrows(IllegalArgumentException.class, () -> summary.getMaxExecutionConcurrency(ConcurrencyType.LEADERSHIP));
    assertThrows(IllegalArgumentException.class, () -> summary.getMinExecutionConcurrency(ConcurrencyType.LEADERSHIP));
    assertThrows(IllegalArgumentException.class, () -> summary.getAvgExecutionConcurrency(ConcurrencyType.LEADERSHIP));
  }
}
