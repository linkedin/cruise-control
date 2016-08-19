/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;

/**
 * A class that wraps the execution information of a balancing proposal
 */
public class ExecutionTask implements Comparable<ExecutionTask> {
  // The execution id of the proposal so we can keep track of the task when execute it.
  public final long executionId;
  // The corresponding balancing proposal of this task.
  public final BalancingProposal proposal;

  public ExecutionTask(long executionId, BalancingProposal proposal) {
    this.executionId = executionId;
    this.proposal = proposal;
  }

  /**
   * @return The source broker of this execution task.
   */
  public int sourceBrokerId() {
    return (int) proposal.sourceBrokerId();
  }

  /**
   * @return The destination broker of this execution task.
   */
  public int destinationBrokerId() {
    return (int) proposal.destinationBrokerId();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof ExecutionTask && this.executionId == ((ExecutionTask) o).executionId;
  }

  @Override
  public int hashCode() {
    return (int) executionId;
  }

  @Override
  public String toString() {
    return "{EXE_ID:" + executionId + "," + proposal + "}";
  }

  @Override
  public int compareTo(ExecutionTask o) {
    if (this.executionId > o.executionId) {
      return 1;
    } else if (this.executionId == o.executionId) {
      return 0;
    } else {
      return -1;
    }
  }
}
