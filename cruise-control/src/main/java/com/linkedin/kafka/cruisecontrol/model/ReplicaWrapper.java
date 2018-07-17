/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import java.util.Objects;
import java.util.function.Function;


/**
 * A class that helps host replica and its score. This class is useful for searching on top of sorted set.
 */
public class ReplicaWrapper implements Comparable<ReplicaWrapper> {
  private final Replica _replica;
  private final double _score;
  private final Function<Replica, Integer> _priorityFunction;

  public ReplicaWrapper(Replica replica, double score, Function<Replica, Integer> priorityFunction) {
    _replica = replica;
    _score = score;
    _priorityFunction = priorityFunction;
  }

  /**
   * @return The score associated with the replica for sorting purpose.
   */
  public double score() {
    return _score;
  }

  /**
   * @return the replica.
   */
  public Replica replica() {
    return _replica;
  }

  @Override
  public String toString() {
    return String.format("(Partition=%s,Broker=%d,Score=%f)",
                         _replica.topicPartition(), _replica.broker().id(), _score);
  }

  @Override
  public int compareTo(ReplicaWrapper o) {
    if (o == null) {
      throw new IllegalArgumentException("Cannot compare to a null object.");
    }
    // First compare the priority function.
    int result = comparePriority(o.replica());
    if (result != 0) {
      return result;
    }
    // Then compare the score.
    result = Double.compare(this.score(), o.score());
    if (result != 0) {
      return result;
    }

    if (this.replica() == Replica.MAX_REPLICA || o.replica() == Replica.MIN_REPLICA) {
      return 1;
    } else if (this.replica() == Replica.MIN_REPLICA || o.replica() == Replica.MAX_REPLICA) {
      return -1;
    } else {
      return this.replica().compareTo(o.replica());
    }
  }

  private int comparePriority(Replica replica) {
    if (_replica == Replica.MAX_REPLICA || _replica == Replica.MIN_REPLICA
        || replica == Replica.MAX_REPLICA || replica == Replica.MIN_REPLICA) {
      return 0;
    }
    if (_priorityFunction != null) {
      int p1 = _priorityFunction.apply(_replica);
      int p2 = _priorityFunction.apply(replica);
      return Integer.compare(p1, p2);
    } else {
      return 0;
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof ReplicaWrapper && ((ReplicaWrapper) obj).score() == _score
        && ((ReplicaWrapper) obj).replica().equals(_replica);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_replica, _score);
  }

  /**
   * Get a ReplicaWrapper for searching purpose on a collection. The returned ReplicaWrapper is:
   * 1. Smaller than any ReplicaWrapper whose score is greater than or equals to the given score.
   * 2. Greater than any ReplicaWrapper whose score is smaller than the given score.
   *
   * @param score the given score.
   * @return A ReplicaWrapper that meets the above condition.
   */
  public static ReplicaWrapper greaterThanOrEqualsTo(double score) {
    return new ReplicaWrapper(Replica.MIN_REPLICA, score, null);
  }

  /**
   * Get a ReplicaWrapper for searching purpose on a collection. The returned ReplicaWrapper is:
   * 1. Smaller than any ReplicaWrapper whose score is greater than the given score.
   * 2. Greater than any ReplicaWrapper whose score is smaller than or equals to the given score.
   *
   * @param score the given score.
   * @return A ReplicaWrapper that meets the above condition.
   */
  public static ReplicaWrapper greaterThan(double score) {
    return new ReplicaWrapper(Replica.MAX_REPLICA, score, null);
  }

  /**
   * Get a ReplicaWrapper for searching purpose on a collection. The returned ReplicaWrapper is:
   * 1. Greater than any ReplicaWrapper whose score is smaller than or equals to the given score.
   * 2. Smaller than any ReplicaWrapper whose score is greater than the given score.
   *
   * @param score the given score.
   * @return A ReplicaWrapper that meets the above condition.
   */
  public static ReplicaWrapper lessThanOrEqualsTo(double score) {
    return new ReplicaWrapper(Replica.MAX_REPLICA, score, null);
  }

  /**
   * Get a ReplicaWrapper for searching purpose on a collection. The returned ReplicaWrapper is:
   * 1. Greater than any ReplicaWrapper whose score is smaller than the given score.
   * 2. Smaller than any ReplicaWrapper whose score is greater than or equals to the given score.
   *
   * @param score the given score.
   * @return A ReplicaWrapper that meets the above condition.
   */
  public static ReplicaWrapper lessThan(double score) {
    return new ReplicaWrapper(Replica.MIN_REPLICA, score, null);
  }
}
