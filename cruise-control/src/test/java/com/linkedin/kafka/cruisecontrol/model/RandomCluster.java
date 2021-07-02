/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.ClusterProperty;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.JBOD_BROKER_CAPACITY_CONFIG_FILE;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.DEFAULT_BROKER_CAPACITY_CONFIG_FILE;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.BROKER_CAPACITY_FETCH_TIMEOUT_MS;

/**
 * A class to generate and populate random clusters with given properties.
 */
public final class RandomCluster {

  // The randomly generated cluster always has this topic and the partition count of this topic
  // equals the broker count in the cluster.
  public static final String TOPIC_WITH_ONE_LEADER_REPLICA_PER_BROKER = "TopicWithOneLeaderPerBroker";

  private RandomCluster() {

  }

  /**
   * Create a random cluster with the given number of racks, brokers and broker capacity.
   *
   * @param clusterProperties Cluster properties specifying number of racks and brokers.
   * @return Cluster with the specified number of racks and brokers.
   * @throws BrokerCapacityResolutionException If broker capacity resolver fails to resolve broker capacity.
   */
  public static ClusterModel generate(Map<ClusterProperty, Number> clusterProperties)
      throws BrokerCapacityResolutionException {
    int numRacks = clusterProperties.get(ClusterProperty.NUM_RACKS).intValue();
    int numBrokers = clusterProperties.get(ClusterProperty.NUM_BROKERS).intValue();
    BrokerCapacityConfigFileResolver configFileResolver = new BrokerCapacityConfigFileResolver();
    boolean populateReplicaPlacementInfo = clusterProperties.get(ClusterProperty.POPULATE_REPLICA_PLACEMENT_INFO).intValue() > 0;
    if (populateReplicaPlacementInfo) {
      configFileResolver.configure(Collections.singletonMap(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE,
                                                            RandomCluster.class.getClassLoader()
                                                                               .getResource(JBOD_BROKER_CAPACITY_CONFIG_FILE).getFile()));
    } else {
      configFileResolver.configure(Collections.singletonMap(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE,
                                                            RandomCluster.class.getClassLoader()
                                                                               .getResource(DEFAULT_BROKER_CAPACITY_CONFIG_FILE).getFile()));
    }

    if (numRacks > numBrokers || numBrokers <= 0 || numRacks <= 0) {
      throw new IllegalArgumentException("Random cluster generation failed due to bad input.");
    }
    // Create cluster.
    ClusterModel cluster = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    // Create racks and add them to cluster.
    for (int i = 0; i < numRacks; i++) {
      cluster.createRack(Integer.toString(i));
    }
    // Create brokers and assign a broker to each rack.
    for (int i = 0; i < numRacks; i++) {
      cluster.createBroker(Integer.toString(i), Integer.toString(i), i,
                           configFileResolver.capacityForBroker("", "", i, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true),
                           populateReplicaPlacementInfo);
    }
    // Assign the rest of the brokers over racks randomly.
    for (int i = numRacks; i < numBrokers; i++) {
      int randomRackId = uniformlyRandom(0, numRacks - 1, TestConstants.SEED_BASE + i);
      cluster.createBroker(Integer.toString(randomRackId), Integer.toString(i), i,
                           configFileResolver.capacityForBroker("", "", i, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true),
                           populateReplicaPlacementInfo);
    }
    return cluster;
  }

  /**
   * Populate the given cluster with replicas having a certain load distribution using the given properties and
   * replica distribution. Balancing constraint sets the resources existing in the cluster at each broker.
   *
   * @param clusterModel The state of the cluster.
   * @param properties Representing the cluster properties as specified in {@link ClusterProperty}.
   * @param replicaDistribution The replica distribution showing the broker of each replica in the cluster.
   */
  public static void populate(ClusterModel clusterModel,
                              Map<ClusterProperty, Number> properties,
                              TestConstants.Distribution replicaDistribution) {
    populate(clusterModel, properties, replicaDistribution, false, true, Collections.emptySet());
  }

  /**
   * Populate the given cluster with replicas having a certain load distribution using the given properties and
   * replica distribution. Balancing constraint sets the resources existing in the cluster at each broker.
   *
   * @param cluster             The state of the cluster.
   * @param properties          Representing the cluster properties as specified in {@link ClusterProperty}.
   * @param replicaDistribution The replica distribution showing the broker of each replica in the cluster.
   * @param rackAware           Whether the replicas should be rack aware or not.
   * @param leaderInFirstPosition Leader of each partition is in the first position or not.
   * @param excludedTopics      Excluded topics.
   */
  public static void populate(ClusterModel cluster,
                              Map<ClusterProperty, Number> properties,
                              TestConstants.Distribution replicaDistribution,
                              boolean rackAware,
                              boolean leaderInFirstPosition,
                              Set<String> excludedTopics) {
    // Sanity checks.
    int numBrokers = cluster.brokers().size();
    int numDeadBrokers = properties.get(ClusterProperty.NUM_DEAD_BROKERS).intValue();
    int numBrokersWithBadDisk = properties.get(ClusterProperty.NUM_BROKERS_WITH_BAD_DISK).intValue();
    boolean populateReplicaPlacementInfo = properties.get(ClusterProperty.POPULATE_REPLICA_PLACEMENT_INFO).intValue() > 0;
    if (numDeadBrokers < 0
        || numBrokersWithBadDisk < 0
        || numBrokers < numDeadBrokers + numBrokersWithBadDisk
        || properties.get(ClusterProperty.MEAN_NW_IN).doubleValue() < 0
        || properties.get(ClusterProperty.MEAN_NW_OUT).doubleValue() < 0
        || properties.get(ClusterProperty.MEAN_DISK).doubleValue() < 0
        || properties.get(ClusterProperty.MEAN_CPU).doubleValue() < 0
        || properties.get(ClusterProperty.NUM_TOPICS).intValue() <= 0
        || properties.get(ClusterProperty.MIN_REPLICATION).intValue() > properties.get(ClusterProperty.MAX_REPLICATION).intValue()
        || (leaderInFirstPosition && properties.get(ClusterProperty.MIN_REPLICATION).intValue() < 2)
        || properties.get(ClusterProperty.MAX_REPLICATION).intValue() > numBrokers
        || properties.get(ClusterProperty.NUM_TOPICS).intValue() > properties.get(ClusterProperty.NUM_REPLICAS).intValue()
        || (properties.get(ClusterProperty.MIN_REPLICATION).intValue() == properties.get(ClusterProperty.MAX_REPLICATION).intValue()
            && properties.get(ClusterProperty.NUM_REPLICAS).intValue() % properties.get(ClusterProperty.MIN_REPLICATION).intValue() != 0)) {
      throw new IllegalArgumentException("Random cluster population failed due to bad input.");
    }

    // Generate topic to number of brokers and replicas distribution.
    List<TopicMetadata> metadata = new ArrayList<>();

    for (int i = 0; i < properties.get(ClusterProperty.NUM_TOPICS).intValue(); i++) {
      metadata.add(new TopicMetadata(i));
    }
    // Increase the replication factor.
    for (int i = 0; i < properties.get(ClusterProperty.NUM_TOPICS).intValue(); i++) {
      int randomReplicationFactor = uniformlyRandom(properties.get(ClusterProperty.MIN_REPLICATION).intValue(),
                                                    properties.get(ClusterProperty.MAX_REPLICATION).intValue(), TestConstants.REPLICATION_SEED + i);
      metadata.get(i).setReplicationFactor(randomReplicationFactor);

      if (totalTopicReplicas(metadata) > properties.get(ClusterProperty.NUM_REPLICAS).intValue()) {
        // Rollback to minimum replicationFactor.
        metadata.get(i).setReplicationFactor(properties.get(ClusterProperty.MIN_REPLICATION).intValue());
      }
    }
    // Increase the number of leaders.
    int maxRandomLeaders =
        properties.get(ClusterProperty.NUM_REPLICAS).intValue() / properties.get(ClusterProperty.NUM_TOPICS).intValue();
    for (int i = 0; i < properties.get(ClusterProperty.NUM_TOPICS).intValue(); i++) {
      int oldNumTopicLeaders = metadata.get(i).numTopicLeaders();
      int randomTopicLeaderCount = uniformlyRandom(2, maxRandomLeaders, TestConstants.LEADER_SEED + i);
      metadata.get(i).setNumTopicLeaders(randomTopicLeaderCount);
      if (totalTopicReplicas(metadata) > properties.get(ClusterProperty.NUM_REPLICAS).intValue()) {
        // Rollback to previous number of topic leaders.
        metadata.get(i).setNumTopicLeaders(oldNumTopicLeaders);
      }
    }
    int totalTopicReplicas = totalTopicReplicas(metadata);
    // Fill in the remaining empty spots.
    while (totalTopicReplicas < properties.get(ClusterProperty.NUM_REPLICAS).intValue()) {

      for (int i = 0; i < properties.get(ClusterProperty.NUM_TOPICS).intValue(); i++) {
        metadata.get(i).incrementNumTopicLeaders();

        totalTopicReplicas = totalTopicReplicas(metadata);
        if (totalTopicReplicas > properties.get(ClusterProperty.NUM_REPLICAS).intValue()) {
          // Rollback to previous number of topic leaders.
          metadata.get(i).decrementNumTopicLeaders();
          totalTopicReplicas = totalTopicReplicas(metadata);
        }

        if (totalTopicReplicas == properties.get(ClusterProperty.NUM_REPLICAS).intValue()) {
          break;
        }
      }
    }
    // Create replicas and set their distribution
    int replicaIndex = 0;
    Map<Resource, Random> randomByResource = new HashMap<>();
    for (Resource resource : Resource.cachedValues()) {
      long seed = TestConstants.UTILIZATION_SEED_BY_RESOURCE.get(resource);
      randomByResource.put(resource, new Random(seed));
    }
    Random randomForTopicPopularity = new Random(TestConstants.TOPIC_POPULARITY_SEED);
    metadata.add(new TopicMetadata(TOPIC_WITH_ONE_LEADER_REPLICA_PER_BROKER, 2, numBrokers));
    for (TopicMetadata datum : metadata) {
      double topicPopularity = exponentialRandom(1.0, randomForTopicPopularity);
      String topic = datum.topic();
      for (int i = 1; i <= datum.numTopicLeaders(); i++) {
        Set<Integer> replicaBrokerIds = new HashSet<>();
        Set<String> replicaRacks = new HashSet<>();
        int brokerConflictResolver = 0;
        TopicPartition pInfo = new TopicPartition(topic, i - 1);
        for (int j = 1; j <= datum.replicationFactor(); j++) {
          int randomBrokerId;

          if (replicaDistribution.equals(TestConstants.Distribution.UNIFORM)) {
            randomBrokerId = uniformlyRandom(0, numBrokers - 1, TestConstants.REPLICA_ASSIGNMENT_SEED + replicaIndex);
            while (replicaBrokerIds.contains(randomBrokerId)
                   || (rackAware && replicaRacks.contains(cluster.broker(randomBrokerId).rack().id()))) {
              brokerConflictResolver++;
              randomBrokerId = uniformlyRandom(0, numBrokers - 1,
                                               TestConstants.REPLICA_ASSIGNMENT_SEED + replicaIndex + brokerConflictResolver);
            }
          } else if (replicaDistribution.equals(TestConstants.Distribution.LINEAR)) {
            int binRange = (numBrokers * (numBrokers + 1)) / 2;
            int randomBinValue = uniformlyRandom(1, binRange, TestConstants.REPLICA_ASSIGNMENT_SEED + replicaIndex);
            randomBrokerId = 0;
            for (int bin = 1; bin <= numBrokers; bin++) {
              int binValue = (2 * randomBinValue);
              if (binValue <= bin * (bin + 1) && binValue > (bin - 1) * bin) {
                randomBrokerId = bin - 1;
                break;
              }
            }

            while (replicaBrokerIds.contains(randomBrokerId)
                   || (rackAware && replicaRacks.contains(cluster.broker(randomBrokerId).rack().id()))) {
              brokerConflictResolver++;
              randomBinValue = uniformlyRandom(1, binRange,
                                               TestConstants.REPLICA_ASSIGNMENT_SEED + replicaIndex + brokerConflictResolver);

              for (int bin = 1; bin <= numBrokers; bin++) {
                int binValue = (2 * randomBinValue);
                if (binValue <= bin * (bin + 1) && binValue > (bin - 1) * bin) {
                  randomBrokerId = bin - 1;
                  break;
                }
              }
            }
          } else {
            // Exponential.
            int binRange = numBrokers * numBrokers;
            int randomBinValue = uniformlyRandom(1, binRange, TestConstants.REPLICA_ASSIGNMENT_SEED + replicaIndex);
            randomBrokerId = 0;
            for (int bin = 1; bin <= numBrokers; bin++) {
              if (randomBinValue <= bin * bin) {
                randomBrokerId = bin - 1;
                break;
              }
            }
            while (replicaBrokerIds.contains(randomBrokerId)
                   || (rackAware && replicaRacks.contains(cluster.broker(randomBrokerId).rack().id()))) {
              brokerConflictResolver++;
              randomBinValue = uniformlyRandom(1, binRange,
                                               TestConstants.REPLICA_ASSIGNMENT_SEED + replicaIndex + brokerConflictResolver);
              for (int bin = 1; bin <= numBrokers; bin++) {
                if (randomBinValue <= bin * bin) {
                  randomBrokerId = bin - 1;
                  break;
                }
              }
            }
          }

          // Set leadership properties and replica load.
          AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues();
          double cpu = exponentialRandom(properties.get(ClusterProperty.MEAN_CPU).doubleValue() * topicPopularity,
                                         randomByResource.get(Resource.CPU));
          KafkaCruiseControlUnitTestUtils.setValueForResource(aggregatedMetricValues, Resource.CPU, cpu);

          double networkInbound = exponentialRandom(properties.get(ClusterProperty.MEAN_NW_IN).doubleValue() * topicPopularity,
                                                    randomByResource.get(Resource.NW_IN));
          KafkaCruiseControlUnitTestUtils.setValueForResource(aggregatedMetricValues, Resource.NW_IN, networkInbound);

          double disk = exponentialRandom(properties.get(ClusterProperty.MEAN_DISK).doubleValue() * topicPopularity,
                                          randomByResource.get(Resource.DISK));
          KafkaCruiseControlUnitTestUtils.setValueForResource(aggregatedMetricValues, Resource.DISK, disk);

          if (j == 1) {
            double networkOutbound = exponentialRandom(properties.get(ClusterProperty.MEAN_NW_OUT).doubleValue() * topicPopularity,
                                                       randomByResource.get(Resource.NW_OUT));
            KafkaCruiseControlUnitTestUtils.setValueForResource(aggregatedMetricValues, Resource.NW_OUT, networkOutbound);
            cluster.createReplica(cluster.broker(randomBrokerId).rack().id(), randomBrokerId, pInfo, j - 1, true);
          } else {
            KafkaCruiseControlUnitTestUtils.setValueForResource(aggregatedMetricValues, Resource.NW_OUT, 0.0);
            cluster.createReplica(cluster.broker(randomBrokerId).rack().id(), randomBrokerId, pInfo, j - 1, false);
          }
          cluster.setReplicaLoad(cluster.broker(randomBrokerId).rack().id(), randomBrokerId, pInfo,
                                 aggregatedMetricValues, Collections.singletonList(1L));

          // Update the set of replica locations.
          replicaBrokerIds.add(randomBrokerId);
          replicaRacks.add(cluster.broker(randomBrokerId).rack().id());
          // Update next replica index
          replicaIndex++;
        }

        // Move leader away from the first position if requested.
        if (!leaderInFirstPosition) {
          Partition partition = cluster.partition(pInfo);
          partition.swapReplicaPositions(1, partition.replicas().indexOf(partition.leader()));
        }
      }
    }
    // Uniform-randomly assign replicas to disks if needed.
    if (populateReplicaPlacementInfo) {
      for (Broker broker : cluster.brokers()) {
        int diskConflictResolver = 0;
        replicaIndex = 0;
        List<Disk> disks = new ArrayList<>(broker.disks());
        for (Replica replica : broker.replicas()) {
          int assignment = uniformlyRandom(0, disks.size() - 1,
                                           TestConstants.REPLICA_ASSIGNMENT_SEED + replicaIndex);
          while (disks.get(assignment).capacity() < disks.get(assignment).utilization() + replica.load().expectedUtilizationFor(Resource.DISK)) {
            diskConflictResolver++;
            assignment = uniformlyRandom(0, disks.size() - 1,
                                         TestConstants.REPLICA_ASSIGNMENT_SEED + replicaIndex + diskConflictResolver);
          }
          disks.get(assignment).addReplica(replica);
          replicaIndex++;
        }
      }
    }

    // Mark dead brokers and brokers with bad disk
    markBrokenBrokers(cluster, numDeadBrokers, numBrokersWithBadDisk, excludedTopics, leaderInFirstPosition, populateReplicaPlacementInfo);
  }

  /**
   * Mark dead brokers: Give priority to marking brokers containing excluded topic replicas. In particular, if the
   * leader is not in the first position in partition replica list, then give priority to brokers containing excluded
   * topic replicas in the current position of the leader -- i.e. 1.
   *
   * Mark brokers with bad disk:
   *
   * @param cluster he state of the cluster.
   * @param numDeadBrokers Number of dead brokers.
   * @param numBrokersWithBadDisk Number of brokers with bad disk.
   * @param excludedTopics Excluded topics.
   * @param leaderInFirstPosition Leader of each partition is in the first position or not.
   * @param populateDisk Whether populate disk information for cluster or not.
   */
  private static void markBrokenBrokers(ClusterModel cluster,
                                        int numDeadBrokers,
                                        int numBrokersWithBadDisk,
                                        Set<String> excludedTopics,
                                        boolean leaderInFirstPosition,
                                        boolean populateDisk) {
    // Mark dead brokers
    if (numDeadBrokers > 0) {
      int markedBrokersContainingExcludedTopicReplicas = 0;

      // Find the brokers with high priority to mark as dead (if any). These brokers are sorted by their id.
      SortedMap<String, List<Partition>> partitionsByTopic = cluster.getPartitionsByTopic();
      SortedSet<Broker> brokersWithExcludedReplicas = new TreeSet<>();
      for (String excludedTopic : excludedTopics) {
        for (Partition excludedPartition : partitionsByTopic.get(excludedTopic)) {
          if (leaderInFirstPosition) {
            brokersWithExcludedReplicas.addAll(excludedPartition.partitionBrokers());
          } else {
            brokersWithExcludedReplicas.add(excludedPartition.replicas().get(1).broker());
          }
        }
      }

      // Mark the brokers with high priority as dead (if any).
      for (Broker brokerToMarkDead : brokersWithExcludedReplicas) {
        cluster.setBrokerState(brokerToMarkDead.id(), Broker.State.DEAD);
        if (++markedBrokersContainingExcludedTopicReplicas >= numDeadBrokers) {
          break;
        }
      }

      // Mark the remaining brokers as dead.
      int remainingDeadBrokerIndex = 0;
      while (numDeadBrokers - markedBrokersContainingExcludedTopicReplicas - remainingDeadBrokerIndex > 0) {
        if (cluster.broker(remainingDeadBrokerIndex).isAlive()) {
          cluster.setBrokerState(remainingDeadBrokerIndex, Broker.State.DEAD);
        }
        remainingDeadBrokerIndex++;
      }
    }
    // Mark brokers with bad disk
    if (numBrokersWithBadDisk > 0) {
      // If cluster has disk information, mark one (random) disk of broker as dead.
      if (populateDisk) {
        int remainingBrokerWithBadDiskIndex = 0;
        for (Broker brokerToMark : cluster.brokers()) {
          if (numBrokersWithBadDisk == remainingBrokerWithBadDiskIndex) {
            break;
          }
          if (brokerToMark.isAlive()) {
            cluster.markDiskDead(brokerToMark.id(), brokerToMark.disks().iterator().next().logDir());
            cluster.setBrokerState(brokerToMark.id(), Broker.State.BAD_DISKS);
            remainingBrokerWithBadDiskIndex++;
          }
        }
        return;
      }

      int markedBrokersContainingExcludedTopicReplicas = 0;

      // Find the brokers with high priority to mark as broker with bad disk (if any). These brokers are sorted by their id.
      SortedMap<String, List<Partition>> partitionsByTopic = cluster.getPartitionsByTopic();
      SortedMap<Integer, Set<Replica>> replicasOnBadDiskByBrokerId = new TreeMap<>();
      for (String excludedTopic : excludedTopics) {
        for (Partition excludedPartition : partitionsByTopic.get(excludedTopic)) {
          for (Replica replica : excludedPartition.replicas().subList(leaderInFirstPosition ? 0 : 1,
                                                                      excludedPartition.replicas().size())) {
            if (!replica.isCurrentOffline()) {
              replicasOnBadDiskByBrokerId.putIfAbsent(replica.broker().id(), new HashSet<>());
              replicasOnBadDiskByBrokerId.get(replica.broker().id()).add(replica);
            }
          }
        }
      }

      // Mark the brokers with high priority as a broker with bad disk(s) (if any).
      for (Map.Entry<Integer, Set<Replica>> entry : replicasOnBadDiskByBrokerId.entrySet()) {
        for (Replica offlineReplica : entry.getValue()) {
          offlineReplica.markOriginalOffline();
        }
        cluster.setBrokerState(entry.getKey(), Broker.State.BAD_DISKS);
        if (++markedBrokersContainingExcludedTopicReplicas >= numBrokersWithBadDisk) {
          break;
        }
      }

      // Mark the remaining brokers as a broker with bad disk(s).
      int remainingBrokerWithBadDiskIndex = 0;
      for (Broker brokerToMark : cluster.brokers()) {
        if (numBrokersWithBadDisk - markedBrokersContainingExcludedTopicReplicas == remainingBrokerWithBadDiskIndex) {
          break;
        }
        if (!brokerToMark.replicas().isEmpty() && brokerToMark.isAlive() && !brokerToMark.hasBadDisks()) {
          // Mark one (random) replica on this broker as offline.
          brokerToMark.replicas().iterator().next().markOriginalOffline();
          cluster.setBrokerState(remainingBrokerWithBadDiskIndex, Broker.State.BAD_DISKS);
          remainingBrokerWithBadDiskIndex++;
        }
      }
      if (numBrokersWithBadDisk - markedBrokersContainingExcludedTopicReplicas != remainingBrokerWithBadDiskIndex) {
        throw new IllegalArgumentException("Broken broker marking failed due to bad input.");
      }
    }
  }

  /**
   * Generates a uniformly random integer in [min, max] using seed.
   *
   * @param min  Minimum value to be returned back.
   * @param max  Maximum value to be returned back.
   * @param seed Seed for the random number generator.
   * @return Random number in the requested range.
   */
  private static int uniformlyRandom(int min, int max, long seed) {
    return (new Random(seed)).nextInt((max - min) + 1) + min;
  }

  /**
   * Generated an exponentially random double with the given mean value using the random object.
   *
   * @param mean Mean value of the exponentially random distribution.
   * @param random Random object to be used for random number generator.
   * @return An exponential random number.
   */
  private static double exponentialRandom(double mean, Random random) {
    return Math.log(1.0 - random.nextDouble()) * (-mean);
  }

  /**
   * Get total number of topic replicas.
   *
   * @param metadata A list of topic metadata.
   * @return Total number of topic replicas.
   */
  private static int totalTopicReplicas(List<TopicMetadata> metadata) {
    int totalTopicReplicas = 0;
    if (metadata == null) {
      return 0;
    }
    for (TopicMetadata datum : metadata) {
      totalTopicReplicas += datum.totalReplicas();
    }
    return totalTopicReplicas;
  }

  /**
   * @return Get a cluster model having a single broker with bad disk.
   */
  public static ClusterModel singleBrokerWithBadDisk() throws BrokerCapacityResolutionException {
    Map<ClusterProperty, Number> singleBrokerWithBadDisk = new HashMap<>();
    singleBrokerWithBadDisk.put(ClusterProperty.NUM_BROKERS, 3);
    singleBrokerWithBadDisk.put(ClusterProperty.NUM_RACKS, 3);
    singleBrokerWithBadDisk.put(ClusterProperty.NUM_BROKERS_WITH_BAD_DISK, 1);
    singleBrokerWithBadDisk.put(ClusterProperty.NUM_REPLICAS, 3);
    singleBrokerWithBadDisk.put(ClusterProperty.NUM_TOPICS, 1);

    Map<ClusterProperty, Number> clusterProperties = new HashMap<>(TestConstants.BASE_PROPERTIES);
    clusterProperties.putAll(singleBrokerWithBadDisk);

    ClusterModel clusterModel = RandomCluster.generate(clusterProperties);
    RandomCluster.populate(clusterModel, clusterProperties, TestConstants.Distribution.UNIFORM, true,
                           true, Collections.emptySet());

    return clusterModel;
  }

  /**
   * A helper class for random cluster generator to keep track of topic related metadata including the name,
   * replication factor, and number of leaders of the topic.
   */
  private static class TopicMetadata {
    private final String _topic;
    private int _replicationFactor;
    private int _numTopicLeaders;

    TopicMetadata(int topicId) {
      this("T" + topicId, 1, 1);
    }
    TopicMetadata(String topicName, int replicationFactor, int numTopicLeaders) {
      _topic = topicName;
      _replicationFactor = replicationFactor;
      _numTopicLeaders = numTopicLeaders;
    }

    void incrementNumTopicLeaders() {
      _numTopicLeaders++;
    }

    void decrementNumTopicLeaders() {
      _numTopicLeaders--;
    }

    String topic() {
      return _topic;
    }

    int numTopicLeaders() {
      return _numTopicLeaders;
    }

    int replicationFactor() {
      return _replicationFactor;
    }

    void setReplicationFactor(int replicationFactor) {
      _replicationFactor = replicationFactor;
    }

    int totalReplicas() {
      return _replicationFactor * _numTopicLeaders;
    }

    void setNumTopicLeaders(int numTopicLeaders) {
      _numTopicLeaders = numTopicLeaders;
    }

    @Override
    public String toString() {
      return String.format("<TopicMetadata name=\"%s\" replicationFactor=\"%d\" " + "numTopicLeaders=\"%d\">"
                           + "%n</TopicMetadata>%n", _topic, _replicationFactor, _numTopicLeaders);
    }
  }
}
