/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.NavigableSet;
import java.util.Random;
import java.util.function.Function;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link SortedReplicas}
 */
public class SortedReplicasTest {
  private static final String SORT_NAME = "sortName";
  private static final Random RANDOM = new Random(0xDEADBEEF);

  private static final Function<Replica, Boolean> SELECTION_FUNC = Replica::isLeader;
  private static final Function<Replica, Integer> PRIORITY_FUNC = r -> r.topicPartition().partition() % 5;
  private static final Function<Replica, Double> SCORE_FUNC = r -> RANDOM.nextDouble();

  private static final int NUM_REPLICAS = 100;

  @Test
  public void testAddAndRemove() {
    Broker broker = generateBroker(NUM_REPLICAS);
    broker.trackSortedReplicas(SORT_NAME, SELECTION_FUNC, PRIORITY_FUNC, SCORE_FUNC);
    SortedReplicas sr = broker.trackedSortedReplicas(SORT_NAME);

    int numReplicas = sr.sortedReplicas().size();
    Replica replica1 = new Replica(new TopicPartition(TOPIC0, 105), broker, false);
    sr.add(replica1);
    assertEquals("The selection function should have filtered out the replica",
                 numReplicas, sr.sortedReplicas().size());

    Replica replica2 = new Replica(new TopicPartition(TOPIC0, 103), broker, true);
    sr.add(replica2);
    assertEquals("The replica should have been added.", numReplicas + 1, sr.sortedReplicas().size());

    verifySortedReplicas(sr);

    // Removing a none existing replica should not throw exception.
    sr.remove(replica1);
    assertEquals(numReplicas + 1, sr.sortedReplicas().size());
    verifySortedReplicas(sr);

    // Remove an existing replica
    sr.remove(replica2);
    assertEquals(numReplicas, sr.sortedReplicas().size());
    verifySortedReplicas(sr);
  }

  @Test
  public void testLazyInitialization() {
    Broker broker = generateBroker(NUM_REPLICAS);
    broker.trackSortedReplicas(SORT_NAME, null, null, SCORE_FUNC);
    SortedReplicas sr = broker.trackedSortedReplicas(SORT_NAME);

    assertEquals("The replicas should be sorted lazily", 0, sr.numReplicas());
    Replica replica = new Replica(new TopicPartition(TOPIC0, 105), broker, false);
    sr.add(replica);
    assertEquals("The replicas should be sorted lazily", 0, sr.numReplicas());
    sr.remove(replica);
    assertEquals("The replicas should be sorted lazily", 0, sr.numReplicas());
    List<Replica> sortedReplicas = sr.sortedReplicas();
    assertEquals("There should be ", NUM_REPLICAS, sortedReplicas.size());
    assertEquals("The replicas should now be sorted", NUM_REPLICAS, sr.numReplicas());
  }

  @Test
  public void testScoreFunctionOnly() {
    Broker broker = generateBroker(NUM_REPLICAS);
    broker.trackSortedReplicas(SORT_NAME, null, null, SCORE_FUNC);
    SortedReplicas sr = broker.trackedSortedReplicas(SORT_NAME);

    double lastScore = Double.NEGATIVE_INFINITY;
    for (ReplicaWrapper rw : sr.sortedReplicaWrappers()) {
      assertTrue(rw.score() >= lastScore);
    }
  }

  @Test
  public void testPriorityFunction() {
    Broker broker = generateBroker(NUM_REPLICAS);
    broker.trackSortedReplicas(SORT_NAME, null, PRIORITY_FUNC, SCORE_FUNC);
    SortedReplicas sr = broker.trackedSortedReplicas(SORT_NAME);

    assertEquals(NUM_REPLICAS, sr.sortedReplicas().size());

    verifySortedReplicas(sr);
  }

  @Test
  public void testSelectionFunction() {
    Broker broker = generateBroker(NUM_REPLICAS);
    broker.trackSortedReplicas(SORT_NAME, SELECTION_FUNC, PRIORITY_FUNC, SCORE_FUNC);
    SortedReplicas sr = broker.trackedSortedReplicas(SORT_NAME);

    assertEquals(broker.leaderReplicas().size(), sr.sortedReplicas().size());

    verifySortedReplicas(sr);
  }

  private void verifySortedReplicas(SortedReplicas sr) {
    int lastPriority = -1;
    double lastScore = Double.NEGATIVE_INFINITY;
    int totalNumPriorities = 0;
    NavigableSet<ReplicaWrapper> sortedReplicas = sr.sortedReplicaWrappers();
    for (ReplicaWrapper rw : sortedReplicas) {
      // Check the selection correctness.
      if (sr.selectionFunction() != null) {
        assertTrue(SELECTION_FUNC.apply(rw.replica()));
      }
      // Check the prioritization correctness.
      if (sr.priorityFunction() != null) {
        int priority = PRIORITY_FUNC.apply(rw.replica());
        assertTrue(lastPriority <= priority);
      }
      // Check the score sorting correctness.
      if (sr.priorityFunction() != null && lastPriority < PRIORITY_FUNC.apply(rw.replica())) {
        lastPriority = PRIORITY_FUNC.apply(rw.replica());
        lastScore = rw.score();
        totalNumPriorities++;
      } else {
        assertTrue(lastScore <= rw.score());
      }
    }

    if (sr.priorityFunction() != null) {
      assertEquals(5, totalNumPriorities);
    }
  }

  private Broker generateBroker(int numReplicas) {
    Rack rack = new Rack("rack");
    Host host = new Host("host", rack);
    Broker broker = new Broker(host, 0, TestConstants.BROKER_CAPACITY);

    for (int i = 0; i < numReplicas; i++) {
      Replica r = new Replica(new TopicPartition(TOPIC0, i), broker, i % 3 == 0);
      broker.addReplica(r);
    }
    return broker;
  }
}
