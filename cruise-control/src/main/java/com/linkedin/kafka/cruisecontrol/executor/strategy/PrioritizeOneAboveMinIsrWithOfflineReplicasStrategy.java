/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionUtils;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;


/**
 * The strategy, which tries to move replicas of OneAboveMinISR partitions with offline replicas.
 * When using this strategy, it should always chain after {@link PrioritizeMinIsrWithOfflineReplicasStrategy}.
 * When multiple brokers are offline, OneAboveMinISR partitions are at a higher risk -- i.e. further failures can make
 * them at the edge of unavailable for produce and consume. This strategy aims to help fixing such partitions faster.
 *
 */
public class PrioritizeOneAboveMinIsrWithOfflineReplicasStrategy extends AbstractReplicaMovementStrategy {

  /** TODO
   * <ul>
   *   <li>If both tasks are moving either an AtMinISR or an UnderMinISR partition, then they have the same priority</li>
   *   <li>If neither task is moving an (At/Under)MinISR partition, then they have the same priority</li>
   *   <li>If a task is moving an UnderMinISR partition, but the other task is not, then task with UnderMinISR partition is prioritized</li>
   *   <li>If neither task is moving an UnderMinISR partition, then the task moving an AtMinISR partition (if any) is prioritized</li>
   * </ul>
   * @param strategyOptions Strategy options to be used while comparing the tasks.
   * @return The comparator of task.
   */
  @Override
  public Comparator<ExecutionTask> taskComparator(StrategyOptions strategyOptions) {
    Comparator<PartitionInfo> comparator = Comparator.comparing(PartitionInfo::topic).thenComparingInt(PartitionInfo::partition);
    Set<PartitionInfo> oneAboveMinIsr = new TreeSet<>(comparator);
    ExecutionUtils.populateMinIsrState(strategyOptions.cluster(), strategyOptions.minIsrWithTimeByTopic(), null, null, oneAboveMinIsr, true);

    return (task1, task2) -> {
      boolean task1IsOneAboveMinISR = isTaskInSet(task1, oneAboveMinIsr);
      boolean task2IsOneAboveMinISR = isTaskInSet(task2, oneAboveMinIsr);

      if (task1IsOneAboveMinISR) {
        // task1 is OneAboveMinIsr. Unless task2 is also OneAboveMinIsr, task1 is prioritized.
        return task2IsOneAboveMinISR ? PRIORITIZE_NONE : PRIORITIZE_TASK_1;
      } else {
        return task2IsOneAboveMinISR ? PRIORITIZE_TASK_2 : PRIORITIZE_NONE;
      }
    };
  }

  @Override
  public Comparator<ExecutionTask> taskComparator(Cluster cluster) {
    return taskComparator(new StrategyOptions.Builder(cluster).build());
  }

  @Override
  public String name() {
    return PrioritizeOneAboveMinIsrWithOfflineReplicasStrategy.class.getSimpleName();
  }
}
