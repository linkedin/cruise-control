/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.common.Resource.*;


/**
 * This disk usage distribution goal should only be used together with KafkaAssignerEvenRackAwareGoal.
 */
public class KafkaAssignerDiskUsageDistributionGoal implements Goal {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaAssignerDiskUsageDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  private BalancingConstraint _balancingConstraint;
  private double _minMonitoredPartitionPercentage = 0.995;
  
  public KafkaAssignerDiskUsageDistributionGoal() {
    
  }

  /**
   * Package private constructor for unit test.
   * @param constraint the balancing constraint.
   */
  KafkaAssignerDiskUsageDistributionGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new DiskDistributionGoalStatsComparator();
  }
  
  @Override
  public void configure(Map<String, ?> configs) {
    _balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(configs, false));
    String minMonitoredPartitionPercentageString =
        (String) configs.get(KafkaCruiseControlConfig.MIN_MONITORED_PARTITION_PERCENTAGE_CONFIG);
    if (minMonitoredPartitionPercentageString != null
        && !minMonitoredPartitionPercentageString.isEmpty()) {
      _minMonitoredPartitionPercentage = Double.parseDouble(minMonitoredPartitionPercentageString);
    }
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(1, _minMonitoredPartitionPercentage, true);
  }

  @Override
  public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, Set<String> excludedTopics)
      throws KafkaCruiseControlException {
    double meanDiskUsage = clusterModel.load().expectedUtilizationFor(DISK) / clusterModel.capacityFor(DISK);
    double upperThreshold = meanDiskUsage * (1 + balancePercentageWithMargin());
    double lowerThreshold = meanDiskUsage * Math.max(0, (1 - balancePercentageWithMargin()));

    Comparator<Broker> comparator = (b1, b2) -> {
      int result = Double.compare(diskUsage(b2), diskUsage(b1));
      return result == 0 ? Integer.compare(b1.id(), b2.id()) : result;
    };
    
    boolean improved;
    int numIterations = 0;
    do {
      List<Broker> brokers = new ArrayList<>();
      brokers.addAll(clusterModel.healthyBrokers());
      brokers.sort(comparator);

      improved = false;
      LOG.debug("Starting iteration {}", numIterations);
      for (Broker broker : brokers) {
        if (checkAndOptimize(broker, clusterModel, meanDiskUsage, lowerThreshold, upperThreshold, excludedTopics)) {
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
   * @return true if all the brokers are within thresholds, false otherwise.
   */
  private boolean isOptimized(ClusterModel clusterModel, double upperThreshold, double lowerThreshold) {
    // Check if any broker is out of the allowed usage range.
    Set<Broker> brokersAboveUpperThreshold = new HashSet<>();
    Set<Broker> brokersUnderLowerThreshold = new HashSet<>();
    for (Broker broker : clusterModel.healthyBrokers()) {
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
               brokersUnderLowerThreshold.size(), lowerThreshold, joiner.toString());
    }
    if (!brokersAboveUpperThreshold.isEmpty()) {
      StringJoiner joiner = new StringJoiner(", ");
      brokersAboveUpperThreshold.forEach(b -> joiner.add(String.format("%d:(%.3f)", b.id(), diskUsage(b))));
      LOG.warn("There are still {} brokers above the upper threshold of {}. The brokers are {}",
               brokersAboveUpperThreshold.size(), upperThreshold, joiner.toString());
    }
    return brokersUnderLowerThreshold.isEmpty() && brokersAboveUpperThreshold.isEmpty();
  }

  /**
   * Optimize the broker if the disk usage of the broker is not within the required range.
   * 
   * @param broker the broker to optimize
   * @param clusterModel the cluster model
   * @param meanDiskUsage the average disk usage of the cluster
   * @param lowerThreshold the lower limit of the disk usage for a broker
   * @param upperThreshold the upper limit of the disk usage for a broker
   * @param excludedTopics the topics to exclude from movement.
   *                       
   * @return true if an action has been taken to improve the disk usage of the broker, false when a broker cannot or
   * does not need to be improved further.
   * 
   * @throws AnalysisInputException
   */
  private boolean checkAndOptimize(Broker broker,
                                   ClusterModel clusterModel,
                                   double meanDiskUsage,
                                   double lowerThreshold,
                                   double upperThreshold,
                                   Set<String> excludedTopics) throws AnalysisInputException {
    LOG.trace("Optimizing broker {}. BrokerDiskUsage = {}, meanDiskUsage = {}", 
              broker, diskUsage(broker), meanDiskUsage);
    double brokerDiskUsage = diskUsage(broker);
    boolean improved = false;
    if (brokerDiskUsage > upperThreshold) {
      LOG.debug("Broker {} disk usage {} is above upper threshold of {}", broker.id(), brokerDiskUsage, upperThreshold);
      List<Broker> brokersAscend = clusterModel.sortedHealthyBrokersUnderThreshold(DISK, brokerDiskUsage);
      for (Broker toSwapWith : brokersAscend) {
        if (toSwapWith == broker) {
          continue;
        }
        if (swapReplicas(broker, toSwapWith, meanDiskUsage, clusterModel, excludedTopics)) {
          improved = true;
          break;
        }
      }
    } else if (brokerDiskUsage < lowerThreshold) {
      LOG.debug("Broker {} disk usage {} is below lower threshold of {}", broker.id(), brokerDiskUsage, lowerThreshold);
      List<Broker> brokersDescend = sortedBrokersAboveSizeDescend(clusterModel, brokerDiskUsage);
      for (Broker toSwapWith : brokersDescend) {
        if (toSwapWith == broker) {
          continue;
        }
        if (swapReplicas(broker, toSwapWith, meanDiskUsage, clusterModel, excludedTopics)) {
          improved = true;
          break;
        }
      }
    }
    return improved;
  }

  /**
   * Swap replica between two brokers. The method should achieve the result that the overall usage of the two 
   * brokers are improved. More specifically, the following result is reduced.
   * <pre>
   * (<tt>UsageOfBroker1</tt> - <tt>MeanUsage</tt>) + (<tt>UsageOfBroker2</tt> - <tt>MeanUsage</tt>)
   * </pre>
   * 
   * @param toSwap the broker that needs to swap a replica with the other broker.
   * @param toSwapWith the broker that provides a replica to swap with the broker <tt>toSwap</tt>
   * @param meanDiskUsage the average usage of the cluster.
   * @param clusterModel the cluster model.
   * @param excludedTopics the topics to exclude from swapping.                    
   * @return true if a swap has been done, false otherwise.
   * 
   * @throws AnalysisInputException
   */
  boolean swapReplicas(Broker toSwap, 
                       Broker toSwapWith, 
                       double meanDiskUsage, 
                       ClusterModel clusterModel,
                       Set<String> excludedTopics) throws AnalysisInputException {
    LOG.trace("Swapping replicas between broker {}({}) and broker {}({})",
             toSwap.id(), brokerSize(toSwap), toSwapWith.id(), brokerSize(toSwapWith));
    double sizeToChange = toSwap.capacityFor(DISK) * meanDiskUsage - brokerSize(toSwap);
    List<ReplicaWrapper> sortedReplicasToSwap = sortReplicasAscend(toSwap.replicas(), excludedTopics);
    List<ReplicaWrapper> sortedLeadersToSwapWith = sortReplicasAscend(toSwapWith.leaderReplicas(), excludedTopics);
    List<ReplicaWrapper> sortedFollowersToSwapWith = sortReplicasAscend(followerReplicas(toSwapWith), excludedTopics);
    
    int startPos;
    int delta;
    if (sizeToChange > 0) {
      // iterate from small replicas to large replicas.
      startPos = 0;
      delta = 1;
    } else {
      // iterate from large replicas to small replicas.
      startPos = sortedReplicasToSwap.size() - 1;
      delta = -1;
    }
    
    for (int i = startPos; i >= 0 && i < sortedReplicasToSwap.size(); i += delta) {
      Replica replicaToSwap = sortedReplicasToSwap.get(i).replica();
      if (excludedTopics.contains(replicaToSwap.topicPartition().topic())) {
        continue;
      }
      // First make sure the replica is possible to be moved to the broker toSwapWith. If this check fails,
      // don't bother to search for replica to swap with.
      if (!possibleToMove(replicaToSwap, toSwapWith, clusterModel)) {
        continue;
      }
      List<ReplicaWrapper> sortedReplicasToSwapWith = 
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
        double maxSizeOfBrokerToSwap = diskUsage(toSwapWith) * toSwap.capacityFor(DISK);
        double currentSizeOfBrokerToSwap = brokerSize(toSwap);
        // after given out the sizeToSwap, the maximum size the broker toSwap can take in.
        maxSize = Math.min(maxSize, maxSizeOfBrokerToSwap - (currentSizeOfBrokerToSwap - sizeToSwap));
        // requirement 3
        double minSizeOfBrokerToSwapWith = diskUsage(toSwap) * toSwapWith.capacityFor(DISK);
        double currentSizeOfBrokerToSwapWith = brokerSize(toSwapWith);
        // after take in the sizeToSwap, the maximum size the broker toSwapWith can give out.
        maxSize = Math.min(maxSize, (currentSizeOfBrokerToSwapWith + sizeToSwap) - minSizeOfBrokerToSwapWith);
      } else {
        // requirement 4
        maxSize = sizeToSwap;
        // requirement 5
        double minSizeOfBrokerToSwap = diskUsage(toSwapWith) * toSwap.capacityFor(DISK);
        double currentSizeOfBrokerToSwap = brokerSize(toSwap);
        // After give out the sizeToSwap, the minimum size the broker toSwap should take in.
        minSize = Math.max(minSize, minSizeOfBrokerToSwap - (currentSizeOfBrokerToSwap - sizeToSwap));
        // requirement 6
        double maxSizeOfBrokerToSwapWith = diskUsage(toSwap) * toSwapWith.capacityFor(DISK);
        double currentSizeOfBrokerToSwapWith = brokerSize(toSwapWith);
        // after take in the sizeToSwap, the minimum size the broker toSwapWith should give out.
        minSize = Math.max(minSize, (currentSizeOfBrokerToSwapWith + sizeToSwap) - maxSizeOfBrokerToSwapWith);
      }
      
      // The target size might be negative here. It would still work for our binary search purpose.
      double targetSize = sizeToSwap + sizeToChange;
      
      // Find a replica that is eligible for swap.
      LOG.trace("replicaToSwap: {}(size={}), targetSize={}, minSize={}, maxSize={}", 
                replicaToSwap, replicaSize(replicaToSwap), targetSize, minSize, maxSize);
      Replica replicaToSwapWith = 
          findReplicaToSwapWith(replicaToSwap, sortedReplicasToSwapWith, targetSize, minSize, maxSize, clusterModel);
      if (replicaToSwapWith != null) {
        LOG.debug("Found replica to swap. Swapping {}({}) on broker {}({}) and {}({}) on broker {}({})", 
                  replicaToSwap.topicPartition(), replicaSize(replicaToSwap), toSwap.id(), brokerSize(toSwap), 
                  replicaToSwapWith.topicPartition(), replicaSize(replicaToSwapWith), toSwapWith.id(), 
                  brokerSize(toSwapWith));
        clusterModel.relocateReplica(replicaToSwapWith.topicPartition(), toSwapWith.id(), toSwap.id());
        clusterModel.relocateReplica(replicaToSwap.topicPartition(), toSwap.id(), toSwapWith.id());
        return true;
      }
    }
    LOG.trace("Nothing to swap between broker {} and broker {}", toSwap.id(), toSwapWith.id());
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
   * @return the replica that can be swapped with the given replica, null otherwise.
   */
  Replica findReplicaToSwapWith(Replica replica,
                                List<ReplicaWrapper> sortedReplicasToSearch,
                                double targetSize,
                                double minSize,
                                double maxSize,
                                ClusterModel clusterModel) {
    int pos = findReplicaPos(sortedReplicasToSearch, targetSize, 0);
    int minPos = findReplicaPos(sortedReplicasToSearch, minSize, 1);
    int maxPos = findReplicaPos(sortedReplicasToSearch, maxSize, -1);
    if (minPos > maxPos) {
      return null;
    }
    // It is possible that the target size is out of the range of minSize and maxSize. In that case, we make it become
    // the closest one in range.
    pos = Math.max(pos, minPos);
    pos = Math.min(pos, maxPos);

    // The following logic starts from pos and searches higher and lower position until it finds a replica that 
    // is eligible to swap with.
    int low = pos;
    int high = pos;
    while (pos >= minPos && pos <= maxPos) {
      Replica toSwapWith = sortedReplicasToSearch.get(pos).replica();
      if (canSwap(replica, sortedReplicasToSearch.get(pos).replica(), clusterModel)) {
        // found the candidate.
        return toSwapWith;
      } else {
        // get the next position.
        pos = findNextPos(sortedReplicasToSearch, targetSize, low - 1, high + 1, minPos, maxPos);
        if (pos == low - 1) {
          low--;
        } else {
          high++;
        }
      }
    }
    return null;
  }

  /**
   * Find the next position between the provided low and high position, whichever is closer to the target size.
   * 
   * @param sortedReplicas the replica list sorted in ascending order based on size.
   * @param targetSize the target size of replica to search
   * @param low the low pointer candidate
   * @param high the high pointer candidate
   * @param minPos the minimum allowed position
   * @param maxPos the maximum allowed position
   *               
   * @return the position chosen between high and low.
   */
  private int findNextPos(List<ReplicaWrapper> sortedReplicas, double targetSize, int low, int high, int minPos, int maxPos) {
    if (low < minPos) {
      return high;
    } else if (high > maxPos) {
      return low;
    } else {
      double leftSizeDiff = Math.abs(replicaSize(sortedReplicas.get(low).replica()) - targetSize);
      double rightSizeDiff = Math.abs(replicaSize(sortedReplicas.get(high).replica()) - targetSize);
      return leftSizeDiff <= rightSizeDiff ? low : high;
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
   * @return true if it is possible to move the replica to the broker, false otherwise.
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
   * @return true if the two replicas can be swapped, false otherwise.
   */
  boolean canSwap(Replica r1, Replica r2, ClusterModel clusterModel) {
    boolean inSameRack = r1.broker().rack() == r2.broker().rack() && r1.broker() != r2.broker();
    boolean rackAware = !clusterModel.partition(r1.topicPartition()).partitionRacks().contains(r2.broker().rack()) 
        && !clusterModel.partition(r2.topicPartition()).partitionRacks().contains(r1.broker().rack());
    boolean sameRole = r1.isLeader() == r2.isLeader();
    return (inSameRack || rackAware) && sameRole;
  }

  /**
   * Find the position of the target size in the sorted replica list. The position is the index of the replica whose
   * size is closest to but greater than (or less than) the target size.
   * 
   * @param sortedReplicas the replica list sorted in ascending order based on the size.
   * @param targetSize the replica size to find.
   * @param shiftOnExactMatch the position to shift if there is an exact match. The value should be 1 or 0 or -1.
   *                          When the value is 1, the method returns the position of the first replica whose size is 
   *                          greater than the target size.
   *                          When the value is 0, the method returns the position of the first replica whose size is 
   *                          greater than or equals to the target size.
   *                          When the value is -1, the method returns the position of the replica whose size is 
   *                          just less than the target size.
   *                          
   * @return the index of the replica whose size is closest but greater than or equals to (shiftOnExactMatch = 0)
   * or greater than (shiftOnExactMatch = 1) or less than (shiftOnExactMatch = -1) the target size.
   */
  private int findReplicaPos(List<ReplicaWrapper> sortedReplicas, double targetSize, int shiftOnExactMatch) {
    if (shiftOnExactMatch != 1 && shiftOnExactMatch != -1 && shiftOnExactMatch != 0) {
      throw new IllegalArgumentException("The shiftOnExactMatch value must be in {-1, 0, 1}");
    }
    int index = Collections.binarySearch(sortedReplicas, new ReplicaWrapper(null, targetSize),
                                         Comparator.comparingDouble(ReplicaWrapper::size));
    switch (shiftOnExactMatch) {
      case -1:
        return index >= 0 ? index - 1 : -(index + 1) - 1;
      case 1:
        return index >= 0 ? index + 1 : -(index + 1);
      case 0:
        return index >= 0 ? index : -(index + 1);
      default:
        throw new IllegalStateException("Invalid shift on exact match value " + shiftOnExactMatch);
    }
  }
  
  private List<Broker> sortedBrokersAboveSizeDescend(ClusterModel clusterModel, double aboveUsage) {
    List<Broker> sortedBrokersDescend = new ArrayList<>();
    List<Broker> sortedBrokersAscend = clusterModel.sortedHealthyBrokersUnderThreshold(DISK, Double.MAX_VALUE);
    for (int i = sortedBrokersAscend.size() - 1; i >= 0; i--) {
      Broker broker = sortedBrokersAscend.get(i);
      if (diskUsage(broker) <= aboveUsage) {
        break;
      }
      sortedBrokersDescend.add(sortedBrokersAscend.get(i));
    }
    return sortedBrokersDescend;
  }

  /**
   * We cannot use {@link Broker#sortedReplicas(Resource)} because the that prioritize the immigrant replicas.
   */
  private List<ReplicaWrapper> sortReplicasAscend(Collection<Replica> replicas, Set<String> excludedTopics) {
    List<ReplicaWrapper> sortedReplicas = new ArrayList<>();
    replicas.forEach(r -> {
      if (!excludedTopics.contains(r.topicPartition().topic())) {
        sortedReplicas.add(new ReplicaWrapper(r, replicaSize(r)));
      }
    });
    sortedReplicas.sort(Comparator.comparingDouble(ReplicaWrapper::size));
    return sortedReplicas;
  }
  
  private List<Replica> followerReplicas(Broker broker) {
    List<Replica> followers = new ArrayList<>();
    broker.replicas().forEach(r -> {
      if (!r.isLeader()) {
        followers.add(r);
      }
    });
    return followers;
  }

  @Override
  public boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel) {
    throw new IllegalStateException("No goal should be executed after " + name());
  }

  @Override
  public String name() {
    return KafkaAssignerDiskUsageDistributionGoal.class.getSimpleName();
  }
  
  private double diskUsage(Broker broker) {
    return broker.load().expectedUtilizationFor(DISK) / broker.capacityFor(DISK);
  }
  
  private double replicaSize(Replica replica) {
    return replica.load().expectedUtilizationFor(DISK);
  }
  
  private double brokerSize(Broker broker) {
    return diskUsage(broker) * broker.capacityFor(DISK);
  }

  /**
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be balancePercentage, we use (balancePercentage-1)*balanceMargin instead.
   * @return the rebalance threshold with a margin.
   */
  private double balancePercentageWithMargin() {
    return (_balancingConstraint.balancePercentage(DISK) - 1) * BALANCE_MARGIN;
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
  static class ReplicaWrapper {
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
  }
}
