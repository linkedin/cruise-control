/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.BrokerAndSortedReplicas;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.ToDoubleFunction;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.common.Resource.DISK;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;


/**
 * This disk usage distribution goal should only be used together with KafkaAssignerEvenRackAwareGoal.
 */
public class KafkaAssignerDiskUsageDistributionGoal implements Goal {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaAssignerDiskUsageDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  // If broker disk usage differ less than USAGE_EQUALITY_DELTA, consider them as equal -- i.e. no swap.
  private static final double USAGE_EQUALITY_DELTA = 0.0001;
  // Ensure that each convergence step due to replica swap is over REPLICA_CONVERGENCE_DELTA.
  private static final double REPLICA_CONVERGENCE_DELTA = 0.4;
  private final ProvisionResponse _provisionResponse;
  private BalancingConstraint _balancingConstraint;
  private double _minMonitoredPartitionPercentage = 0.995;

  public KafkaAssignerDiskUsageDistributionGoal() {
    _provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
  }

  /**
   * Package private constructor for unit test.
   * @param constraint the balancing constraint.
   */
  KafkaAssignerDiskUsageDistributionGoal(BalancingConstraint constraint) {
    this();
    _balancingConstraint = constraint;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new DiskDistributionGoalStatsComparator();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    KafkaCruiseControlConfig parsedConfig = new KafkaCruiseControlConfig(configs, false);
    _balancingConstraint = new BalancingConstraint(parsedConfig);
    _minMonitoredPartitionPercentage = parsedConfig.getDouble(MonitorConfig.MIN_VALID_PARTITION_RATIO_CONFIG);
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, _minMonitoredPartitionPercentage, true);
  }

  @Override
  public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, OptimizationOptions optimizationOptions) {
    KafkaAssignerUtils.sanityCheckOptimizationOptions(optimizationOptions);
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    double meanDiskUsage = clusterModel.load().expectedUtilizationFor(DISK) / clusterModel.capacityFor(DISK);
    double upperThreshold = meanDiskUsage * (1 + balancePercentageWithMargin());
    double lowerThreshold = meanDiskUsage * Math.max(0, (1 - balancePercentageWithMargin()));

    // Create broker comparator in descending order of broker disk usage.
    Comparator<BrokerAndSortedReplicas> brokerComparator =
        Comparator.comparingDouble((ToDoubleFunction<BrokerAndSortedReplicas>) this::diskUsage)
                  .thenComparingInt(bs -> bs.broker().id());
    // Create replica comparator in ascending order of replica disk usage.
    Comparator<Replica> replicaComparator = Comparator.comparingDouble(this::replicaSize).thenComparing(r -> r);

    // create a sorted set for all the brokers.
    SortedSet<BrokerAndSortedReplicas> allBrokers = new TreeSet<>(brokerComparator);
    clusterModel.aliveBrokers().forEach(b -> allBrokers.add(new BrokerAndSortedReplicas(b, replicaComparator)));

    boolean improved;
    int numIterations = 0;
    do {
      improved = false;
      LOG.debug("Starting iteration {}", numIterations);
      // Create another list to avoid ConcurrentModificationException.
      List<BrokerAndSortedReplicas> allBrokerAndSortedReplicas = new ArrayList<>(allBrokers);
      for (BrokerAndSortedReplicas bas : allBrokerAndSortedReplicas) {
        if (checkAndOptimize(allBrokers, bas, clusterModel, meanDiskUsage, lowerThreshold, upperThreshold, excludedTopics)) {
          improved = true;
        }
      }
      numIterations++;
    } while (improved);

    boolean succeeded = isOptimized(clusterModel, upperThreshold, lowerThreshold);
    LOG.debug("Finished optimization in {} iterations.", numIterations);
    return succeeded;
  }

  /**
   * Check whether the cluster model still has brokers whose disk usage are above upper threshold or below lower
   * threshold.
   *
   * @param clusterModel the cluster model to check
   * @param upperThreshold the upper threshold of the disk usage.
   * @param lowerThreshold the lower threshold of the disk usage.
   *
   * @return {@code true} if all the brokers are within thresholds, {@code false} otherwise.
   */
  private boolean isOptimized(ClusterModel clusterModel, double upperThreshold, double lowerThreshold) {
    // Check if any broker is out of the allowed usage range.
    Set<Broker> brokersAboveUpperThreshold = new HashSet<>();
    Set<Broker> brokersUnderLowerThreshold = new HashSet<>();
    for (Broker broker : clusterModel.aliveBrokers()) {
      double diskUsage = diskUsage(broker);
      if (diskUsage < lowerThreshold) {
        brokersUnderLowerThreshold.add(broker);
      } else if (diskUsage > upperThreshold) {
        brokersAboveUpperThreshold.add(broker);
      }
    }
    if (!brokersUnderLowerThreshold.isEmpty()) {
      StringJoiner joiner = new StringJoiner(", ");
      brokersUnderLowerThreshold.forEach(b -> joiner.add(String.format("%d:(%.3f)", b.id(), diskUsage(b))));
      LOG.warn("There are still {} brokers under the lower threshold of {}. The brokers are {}",
               brokersUnderLowerThreshold.size(), dWrap(lowerThreshold), joiner);
    }
    if (!brokersAboveUpperThreshold.isEmpty()) {
      StringJoiner joiner = new StringJoiner(", ");
      brokersAboveUpperThreshold.forEach(b -> joiner.add(String.format("%d:(%.3f)", b.id(), diskUsage(b))));
      LOG.warn("There are still {} brokers above the upper threshold of {}. The brokers are {}",
               brokersAboveUpperThreshold.size(), dWrap(upperThreshold), joiner);
    }
    return brokersUnderLowerThreshold.isEmpty() && brokersAboveUpperThreshold.isEmpty();
  }

  /**
   * Optimize the broker if the disk usage of the broker is not within the required range.
   *
   * @param allBrokers a sorted set of all the alive brokers in the cluster.
   * @param toOptimize the broker to optimize
   * @param clusterModel the cluster model
   * @param meanDiskUsage the average disk usage of the cluster
   * @param lowerThreshold the lower limit of the disk usage for a broker
   * @param upperThreshold the upper limit of the disk usage for a broker
   * @param excludedTopics the topics to exclude from movement.
   *
   * @return {@code true} if an action has been taken to improve the disk usage of the broker, {@code false} when a broker cannot or
   * does not need to be improved further.
   */
  private boolean checkAndOptimize(SortedSet<BrokerAndSortedReplicas> allBrokers,
                                   BrokerAndSortedReplicas toOptimize,
                                   ClusterModel clusterModel,
                                   double meanDiskUsage,
                                   double lowerThreshold,
                                   double upperThreshold,
                                   Set<String> excludedTopics) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Optimizing broker {}. BrokerDiskUsage = {}, meanDiskUsage = {}",
                toOptimize.broker(), dWrap(diskUsage(toOptimize.broker())), dWrap(meanDiskUsage));
    }
    double brokerDiskUsage = diskUsage(toOptimize.broker());
    boolean improved = false;
    List<BrokerAndSortedReplicas> candidateBrokersToSwapWith;

    if (brokerDiskUsage > upperThreshold) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Broker {} disk usage {} is above upper threshold of {}",
                  toOptimize.broker().id(), dWrap(brokerDiskUsage), dWrap(upperThreshold));
      }
      // Get the brokers whose disk usage is less than the broker to optimize. The list is in ascending order based on
      // broker disk usage.
      candidateBrokersToSwapWith = new ArrayList<>(allBrokers.headSet(toOptimize));

    } else if (brokerDiskUsage < lowerThreshold) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Broker {} disk usage {} is below lower threshold of {}",
                  toOptimize.broker().id(), dWrap(brokerDiskUsage), dWrap(lowerThreshold));
      }
      // Get the brokers whose disk usage is more than the broker to optimize. The list is in descending order based on
      // broker disk usage.
      candidateBrokersToSwapWith = new ArrayList<>(allBrokers.tailSet(toOptimize));
      Collections.reverse(candidateBrokersToSwapWith);
    } else {
      // Nothing to optimize.
      return false;
    }

    for (BrokerAndSortedReplicas toSwapWith : candidateBrokersToSwapWith) {
      if (toSwapWith == toOptimize || Math.abs(diskUsage(toSwapWith) - diskUsage(toOptimize)) < USAGE_EQUALITY_DELTA) {
        continue;
      }
      // Remove the brokers involved in swap from the tree set before swap.
      allBrokers.removeAll(Arrays.asList(toOptimize, toSwapWith));
      try {
        if (swapReplicas(toOptimize, toSwapWith, meanDiskUsage, clusterModel, excludedTopics)) {
          improved = true;
          break;
        }
      } finally {
        // Add the brokers back to the tree set after the swap.
        allBrokers.addAll(Arrays.asList(toOptimize, toSwapWith));
      }
    }
    return improved;
  }

  /**
   * Swap replica between two brokers. The method should achieve the result that the overall usage of the two
   * brokers are improved. More specifically, the following result is reduced.
   * <pre>
   * ({@code UsageOfBroker1} - {@code MeanUsage}) + ({@code UsageOfBroker2} - {@code MeanUsage})
   * </pre>
   *
   * @param toSwap the broker that needs to swap a replica with the other broker.
   * @param toSwapWith the broker that provides a replica to swap with the broker {@code toSwap}
   * @param meanDiskUsage the average usage of the cluster.
   * @param clusterModel the cluster model.
   * @param excludedTopics the topics to exclude from swapping.
   * @return {@code true} if a swap has been done, {@code false} otherwise.
   */
  boolean swapReplicas(BrokerAndSortedReplicas toSwap,
                       BrokerAndSortedReplicas toSwapWith,
                       double meanDiskUsage,
                       ClusterModel clusterModel,
                       Set<String> excludedTopics) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Swapping replicas between broker {}({}) and broker {}({})",
                toSwap.broker().id(), dWrap(brokerSize(toSwap)), toSwapWith.broker().id(), dWrap(brokerSize(toSwapWith)));
    }
    double sizeToChange = toSwap.broker().capacityFor(DISK) * meanDiskUsage - brokerSize(toSwap);
    NavigableSet<ReplicaWrapper> sortedReplicasToSwap = sortReplicasAscend(toSwap, excludedTopics);
    NavigableSet<ReplicaWrapper> sortedLeadersToSwapWith = sortReplicasAscend(toSwapWith, excludedTopics);
    NavigableSet<ReplicaWrapper> sortedFollowersToSwapWith = sortedFollowerReplicas(toSwapWith, excludedTopics);

    // Depending on whether we need more or less disk usage, using ascending or descending iterator.
    Iterator<ReplicaWrapper> toSwapIter =
        sizeToChange > 0 ? sortedReplicasToSwap.iterator() : sortedReplicasToSwap.descendingIterator();

    while (toSwapIter.hasNext()) {
      Replica replicaToSwap = toSwapIter.next().replica();
      if (excludedTopics.contains(replicaToSwap.topicPartition().topic())) {
        continue;
      }
      // First make sure the replica is possible to be moved to the broker toSwapWith. If this check fails,
      // don't bother to search for replica to swap with.
      if (!possibleToMove(replicaToSwap, toSwapWith.broker(), clusterModel)) {
        continue;
      }
      NavigableSet<ReplicaWrapper> sortedReplicasToSwapWith =
          replicaToSwap.isLeader() ? sortedLeadersToSwapWith : sortedFollowersToSwapWith;
      double sizeToSwap = replicaSize(replicaToSwap);
      // No need to continue if we are trying to reduce the size and the replicas to swap out is of size 0.
      if (sizeToChange < 0 && sizeToSwap == 0) {
        break;
      }
      // when sizeToChange > 0, the broker toSwap needs more disk utilization, the replicaToSwapWith should meet the
      // following requirements:
      // 1. replicaToSwapWith.size() > replicaToSwap.size()
      // 2. After the swap, the disk usage of broker toSwap should not be more than the disk usage of broker
      //    toSwapWith before the swap.
      // 3. After the swap, the disk usage of broker toSwapWith should not be less than the disk usage of broker
      //    toSwap before the swap.
      //
      // When sizeToChange < 0, the broker toSwap needs less disk utilization, the replicaToSwapWith should meet the
      // following requirements:
      // 4. replicaToSwapWith.size < replicaToSwap.size()
      // 5. After the swap, the disk usage of broker toSwap should not be less than the disk usage of broker
      //    toSwapWith before the swap.
      // 6. After the swap, the disk usage of broker toSwapWith should not be more than the disk usage of broker
      //    toSwap before the swap.
      //
      // We do not require the swap to be under the balance upper limit or lower limit. Instead, we just ensure
      // that after the swap, the two replicas are closer to the mean usage.
      double maxSize = Double.MAX_VALUE;
      double minSize = Double.MIN_VALUE;
      if (sizeToChange > 0) {
        // requirement 1
        minSize = sizeToSwap;
        // requirement 2
        double maxSizeOfBrokerToSwap = diskUsage(toSwapWith) * toSwap.broker().capacityFor(DISK);
        double currentSizeOfBrokerToSwap = brokerSize(toSwap);
        // after given out the sizeToSwap, the maximum size the broker toSwap can take in.
        maxSize = Math.min(maxSize, maxSizeOfBrokerToSwap - (currentSizeOfBrokerToSwap - sizeToSwap));
        // requirement 3
        double minSizeOfBrokerToSwapWith = diskUsage(toSwap) * toSwapWith.broker().capacityFor(DISK);
        double currentSizeOfBrokerToSwapWith = brokerSize(toSwapWith);
        // after take in the sizeToSwap, the maximum size the broker toSwapWith can give out.
        maxSize = Math.min(maxSize, (currentSizeOfBrokerToSwapWith + sizeToSwap) - minSizeOfBrokerToSwapWith);
      } else {
        // requirement 4
        maxSize = sizeToSwap;
        // requirement 5
        double minSizeOfBrokerToSwap = diskUsage(toSwapWith) * toSwap.broker().capacityFor(DISK);
        double currentSizeOfBrokerToSwap = brokerSize(toSwap);
        // After give out the sizeToSwap, the minimum size the broker toSwap should take in.
        minSize = Math.max(minSize, minSizeOfBrokerToSwap - (currentSizeOfBrokerToSwap - sizeToSwap));
        // requirement 6
        double maxSizeOfBrokerToSwapWith = diskUsage(toSwap) * toSwapWith.broker().capacityFor(DISK);
        double currentSizeOfBrokerToSwapWith = brokerSize(toSwapWith);
        // after take in the sizeToSwap, the minimum size the broker toSwapWith should give out.
        minSize = Math.max(minSize, (currentSizeOfBrokerToSwapWith + sizeToSwap) - maxSizeOfBrokerToSwapWith);
      }

      // Add the delta to the min and max size.
      minSize += REPLICA_CONVERGENCE_DELTA;
      maxSize -= REPLICA_CONVERGENCE_DELTA;

      // The target size might be negative here. It would still work for our binary search purpose.
      double targetSize = sizeToSwap + sizeToChange;

      // Find a replica that is eligible for swap.
      if (LOG.isTraceEnabled()) {
        LOG.trace("replicaToSwap: {}(size={}), targetSize={}, minSize={}, maxSize={}",
                  replicaToSwap, dWrap(replicaSize(replicaToSwap)), dWrap(targetSize), dWrap(minSize), dWrap(maxSize));
      }
      Replica replicaToSwapWith = sortedReplicasToSwapWith.isEmpty() ? null : findReplicaToSwapWith(replicaToSwap, sortedReplicasToSwapWith,
                                                                                                    targetSize, minSize, maxSize, clusterModel);
      if (replicaToSwapWith != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found replica to swap. Swapping {}({}) on broker {}({}) and {}({}) on broker {}({})",
                    replicaToSwap.topicPartition(), dWrap(replicaSize(replicaToSwap)), toSwap.broker().id(),
                    dWrap(brokerSize(toSwap)), replicaToSwapWith.topicPartition(), dWrap(replicaSize(replicaToSwapWith)),
                    toSwapWith.broker().id(), dWrap(brokerSize(toSwapWith)));
        }
        clusterModel.relocateReplica(replicaToSwapWith.topicPartition(), toSwapWith.broker().id(), toSwap.broker().id());
        clusterModel.relocateReplica(replicaToSwap.topicPartition(), toSwap.broker().id(), toSwapWith.broker().id());
        toSwap.sortedReplicas().remove(replicaToSwap);
        toSwap.sortedReplicas().add(replicaToSwapWith);
        toSwapWith.sortedReplicas().remove(replicaToSwapWith);
        toSwapWith.sortedReplicas().add(replicaToSwap);
        return true;
      }
    }
    LOG.trace("Nothing to swap between broker {} and broker {}", toSwap.broker().id(), toSwapWith.broker().id());
    return false;
  }

  /**
   * The function searches in a given sorted replica list until it finds a replica that is eligible to swap with the
   * specified replica.
   *
   * @param replica the specific replica to swap.
   * @param sortedReplicasToSearch  the sorted replica list.
   * @param targetSize the target size for the eligible replica.
   * @param minSize the min size for the eligible replica.
   * @param maxSize the max size for the eligible replica.
   * @param clusterModel the cluster model.
   *
   * @return The replica that can be swapped with the given replica, null otherwise.
   */
  Replica findReplicaToSwapWith(Replica replica,
                                NavigableSet<ReplicaWrapper> sortedReplicasToSearch,
                                double targetSize,
                                double minSize,
                                double maxSize,
                                ClusterModel clusterModel) {
    if (minSize > maxSize) {
      return null;
    }

    // Within the candidate replicas, find the replicas whose size falls into [minSize, maxSize], not inclusive.
    NavigableSet<ReplicaWrapper> candidates = sortedReplicasToSearch.subSet(ReplicaWrapper.greaterThan(minSize),
                                                                            false,
                                                                            ReplicaWrapper.lessThan(maxSize),
                                                                            false);
    // No candidate available, just return null.
    if (candidates.isEmpty()) {
      return null;
    }
    // The iterator for replicas from [targetSize (inclusive), maxSize (exclusive)]
    Iterator<ReplicaWrapper> ascendingLargerIter = null;
    // The iterator for replicas from [minSize (exclusive), targetSize (inclusive)]
    Iterator<ReplicaWrapper> descendingLessIter = null;
    // Check if the target size falls in the range or not. This is needed to avoid passing invalid targetSize to
    // the tailSet() or headSet().
    if (targetSize <= minSize) {
      ascendingLargerIter = candidates.iterator();
    } else if (targetSize >= maxSize) {
      descendingLessIter = candidates.descendingIterator();
    } else {
      ascendingLargerIter = candidates.tailSet(ReplicaWrapper.greaterThanOrEqualsTo(targetSize), true).iterator();
      descendingLessIter = candidates.headSet(ReplicaWrapper.lessThanOrEqualsTo(targetSize), true).descendingIterator();
    }

    // Advance the ascending and descending iterator in the ascending order of their distance from target size,
    // return the first replica that can swap. Otherwise return null.
    ReplicaWrapper low = null;
    ReplicaWrapper high = null;
    ReplicaWrapper candidateReplica = null;
    while (true) {
      // The last checked replica is from the high end, advance the ascending iterator.
      if (candidateReplica == high) {
        high = ascendingLargerIter != null && ascendingLargerIter.hasNext() ? ascendingLargerIter.next() : null;
      }
      // The last checked replica is from the low end, advance the descending iterator.
      if (candidateReplica == low) {
        low = descendingLessIter != null && descendingLessIter.hasNext() ? descendingLessIter.next() : null;
      }

      // No more replicas to check, give up.
      if (high == null && low == null) {
        return null;
      } else if (high == null) {
        // Use the lower end
        candidateReplica = low;
      } else if (low == null) {
        // Use the higher end
        candidateReplica = high;
      } else {
        // pick a replica closer to the target.
        double lowDiff = targetSize - low.size();
        double highDiff = high.size() - targetSize;
        candidateReplica = lowDiff <= highDiff ? low : high;
      }
      if (canSwap(replica, candidateReplica.replica(), clusterModel)) {
        return candidateReplica.replica();
      }
    }
  }

  /**
   * Checks if a replica is possible to be moved to a broker.
   *
   * A replica is possible to move to a broker if
   * 1. the rack of the broker does not contain a replica of the same partition, OR
   * 2. the rack of the broker contains the replica, but the replica is not in the given broker.
   *
   * @param replica the replica to move.
   * @param destinationBroker the broker to move the replica to.
   * @param clusterModel the cluster model.
   *
   * @return {@code true} if it is possible to move the replica to the broker, {@code false} otherwise.
   */
  private boolean possibleToMove(Replica replica, Broker destinationBroker, ClusterModel clusterModel) {
    TopicPartition tp = replica.topicPartition();

    boolean case1 = !clusterModel.partition(tp).partitionRacks().contains(destinationBroker.rack());
    boolean case2 = replica.broker().rack() == destinationBroker.rack() && destinationBroker.replica(tp) == null;

    return case1 || case2;
  }

  /**
   * Checks whether the two replicas can swap with each other. Two replicas can swap if:
   * 1. r1 and r2 are in the same rack (this assumes the initial assignment is already rack aware), OR
   * 2. partition of r1 does not have replica in the rack of r2, and vice versa
   *
   * In addition, r1 and r2 must have the same role, i.e. either both are leaders or both are followers.
   * @param r1 the first replica to swap
   * @param r2 the second replica to swap with the first replica
   * @param clusterModel the cluster model
   * @return {@code true} if the two replicas can be swapped, {@code false} otherwise.
   */
  boolean canSwap(Replica r1, Replica r2, ClusterModel clusterModel) {
    boolean inSameRack = r1.broker().rack() == r2.broker().rack() && r1.broker() != r2.broker();
    boolean rackAware = !clusterModel.partition(r1.topicPartition()).partitionRacks().contains(r2.broker().rack())
        && !clusterModel.partition(r2.topicPartition()).partitionRacks().contains(r1.broker().rack());
    boolean sameRole = r1.isLeader() == r2.isLeader();
    return (inSameRack || rackAware) && sameRole;
  }

  private NavigableSet<ReplicaWrapper> sortReplicasAscend(BrokerAndSortedReplicas bas, Set<String> excludedTopics) {
    NavigableSet<ReplicaWrapper> sortedReplicas = new TreeSet<>();
    bas.sortedReplicas().forEach(r -> {
      if (!excludedTopics.contains(r.topicPartition().topic())) {
        sortedReplicas.add(new ReplicaWrapper(r, replicaSize(r)));
      }
    });
    return sortedReplicas;
  }

  private NavigableSet<ReplicaWrapper> sortedFollowerReplicas(BrokerAndSortedReplicas bas, Set<String> excludedTopics) {
    NavigableSet<ReplicaWrapper> sortedFollowers = new TreeSet<>();
    bas.sortedReplicas().forEach(r -> {
      if (!r.isLeader() || excludedTopics.contains(r.topicPartition().topic())) {
        sortedFollowers.add(new ReplicaWrapper(r, replicaSize(r)));
      }
    });
    return sortedFollowers;
  }

  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    throw new IllegalStateException("No goal should be executed after " + name());
  }

  @Override
  public String name() {
    return KafkaAssignerDiskUsageDistributionGoal.class.getSimpleName();
  }

  @Override
  public void finish() {
    // Done.
  }

  @Override
  public boolean isHardGoal() {
    return true;
  }

  @Override
  public ProvisionStatus provisionStatus() {
    // Provision status computation is not supported for kafka_assigner goals.
    return provisionResponse().status();
  }

  @Override
  public ProvisionResponse provisionResponse() {
    return _provisionResponse;
  }

  private double diskUsage(BrokerAndSortedReplicas bas) {
    // Ensure that if the disk capacity is non-positive, we do not report a positive disk usage.
    double diskCapacity = bas.broker().capacityFor(DISK);
    return Double.compare(diskCapacity, 0.0) < 1 ? 0.0 : bas.broker().load().expectedUtilizationFor(DISK) / diskCapacity;
  }

  private double diskUsage(Broker broker) {
    // Ensure that if the disk capacity is non-positive, we do not report a positive disk usage.
    double diskCapacity = broker.capacityFor(DISK);
    return Double.compare(diskCapacity, 0.0) < 1 ? 0.0 : broker.load().expectedUtilizationFor(DISK) / diskCapacity;
  }

  private double replicaSize(Replica replica) {
    return replica.load().expectedUtilizationFor(DISK);
  }

  private double brokerSize(BrokerAndSortedReplicas bas) {
    return diskUsage(bas) * bas.broker().capacityFor(DISK);
  }

  /**
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be resourceBalancePercentage, we use (resourceBalancePercentage-1)*balanceMargin instead.
   * @return The rebalance threshold with a margin.
   */
  private double balancePercentageWithMargin() {
    return (_balancingConstraint.resourceBalancePercentage(DISK) - 1) * BALANCE_MARGIN;
  }

  private DoubleWrapper dWrap(double value) {
    return new DoubleWrapper(value);
  }

  /**
   * This is the same comparator code as the one in
   * {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal}
   */
  private class DiskDistributionGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Number of balanced brokers in the highest priority resource cannot be more than the pre-optimized
      // stats. This constraint is applicable for the rest of the resources, if their higher priority resources
      // have the same number of balanced brokers in their corresponding pre- and post-optimized stats.
      int numBalancedBroker1 = stats1.numBalancedBrokersByResource().get(DISK);
      int numBalancedBroker2 = stats2.numBalancedBrokersByResource().get(DISK);
      // First compare the
      if (numBalancedBroker2 > numBalancedBroker1) {
        _reasonForLastNegativeResult = String.format(
            "Violated %s. [Number of Balanced Brokers] for resource %s. post-optimization:%d pre-optimization:%d",
            name(), DISK, numBalancedBroker1, numBalancedBroker2);
        return -1;
      }
      return 1;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }

  /**
   * A class that helps host partition and its
   * package private for unit tests.
   */
  static class ReplicaWrapper implements Comparable<ReplicaWrapper> {
    private final Replica _replica;
    private final double _size;

    ReplicaWrapper(Replica replica, double size) {
      _replica = replica;
      _size = size;
    }

    private double size() {
      return _size;
    }

    private Replica replica() {
      return _replica;
    }

    @Override
    public int compareTo(ReplicaWrapper o) {
      if (o == null) {
        throw new IllegalArgumentException("Cannot compare to a null object.");
      }
      int result = Double.compare(this.size(), o.size());
      if (result != 0) {
        // Objects to compare have different size.
        return result;
      } else {
        if ((this.replica() == Replica.MAX_REPLICA || this.replica() == Replica.MIN_REPLICA)
            && (o.replica() == Replica.MAX_REPLICA || o.replica() == Replica.MIN_REPLICA)) {
          // Exclusive comparison between MIN_REPLICA and MAX_REPLICA, which has the same size, are equal.
          return 0;
        } else if (this.replica() == Replica.MAX_REPLICA || o.replica() == Replica.MIN_REPLICA) {
          // Given the same size, MAX replica is greater than and MIN replica is smaller than other replicas.
          return 1;
        } else if (this.replica() == Replica.MIN_REPLICA || o.replica() == Replica.MAX_REPLICA) {
          // Given the same size, MAX replica is greater than and MIN replica is smaller than other replicas.
          return -1;
        } else {
          return this.replica().compareTo(o.replica());
        }
      }
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof ReplicaWrapper && ((ReplicaWrapper) obj).size() == _size
          && ((ReplicaWrapper) obj).replica().equals(_replica);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_replica, _size);
    }

    private static ReplicaWrapper greaterThanOrEqualsTo(double size) {
      return new ReplicaWrapper(Replica.MIN_REPLICA, size);
    }

    private static ReplicaWrapper greaterThan(double size) {
      return new ReplicaWrapper(Replica.MAX_REPLICA, size);
    }

    private static ReplicaWrapper lessThanOrEqualsTo(double size) {
      return new ReplicaWrapper(Replica.MAX_REPLICA, size);
    }

    private static ReplicaWrapper lessThan(double size) {
      return new ReplicaWrapper(Replica.MIN_REPLICA, size);
    }
  }

  // Thin wrapper around double for printing purpose.
  private static final class DoubleWrapper {
    private final double _value;

    private DoubleWrapper(double value) {
      _value = value;
    }

    @Override
    public String toString() {
      return Double.toString(_value);
    }
  }
}
