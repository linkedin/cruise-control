/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;

import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;


/**
 * A class to generate and populate random clusters with given properties.
 */
public class RandomCluster {

  private RandomCluster() {

  }

  /**
   * Create a random cluster with the given number of racks, brokers and broker capacity.
   *
   * @param clusterProperties Cluster properties specifying number of racks and brokers.
   * @return Cluster with the specified number of racks and brokers.
   */
  public static ClusterModel generate(Map<ClusterProperty, Number> clusterProperties) {
    int numRacks = clusterProperties.get(ClusterProperty.NUM_RACKS).intValue();
    int numBrokers = clusterProperties.get(ClusterProperty.NUM_BROKERS).intValue();
    BrokerCapacityConfigFileResolver configFileResolver = new BrokerCapacityConfigFileResolver();
    configFileResolver.configure(Collections.singletonMap(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE,
                                                          RandomCluster.class.getClassLoader().getResource("DefaultCapacityConfig.json").getFile()));

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
      cluster.createBroker(Integer.toString(i), Integer.toString(i), i, configFileResolver.capacityForBroker("", "", i));
    }
    // Assign the rest of the brokers over racks randomly.
    for (int i = numRacks; i < numBrokers; i++) {
      int randomRackId = uniformlyRandom(0, numRacks - 1, TestConstants.SEED_BASE + i);
      cluster.createBroker(Integer.toString(randomRackId), Integer.toString(i), i, configFileResolver.capacityForBroker("", "", i));
    }
    return cluster;
  }

  /**
   * Populate the given cluster with replicas having a certain load distribution using the given properties and
   * replica distribution. Balancing constraint sets the resources existing in the cluster at each broker.
   *
   * @param cluster             The state of the cluster.
   * @param properties          Representing the cluster properties as specified in {@link ClusterProperty}.
   * @param replicaDistribution The replica distribution showing the broker of each replica in the cluster.
   */
  public static void populate(ClusterModel cluster,
                              Map<ClusterProperty, Number> properties,
                              TestConstants.Distribution replicaDistribution) {
    populate(cluster, properties, replicaDistribution, false);
  }

  /**
   * Populate the given cluster with replicas having a certain load distribution using the given properties and
   * replica distribution. Balancing constraint sets the resources existing in the cluster at each broker.
   *
   * @param cluster             The state of the cluster.
   * @param properties          Representing the cluster properties as specified in {@link ClusterProperty}.
   * @param replicaDistribution The replica distribution showing the broker of each replica in the cluster.
   * @param rackAware           Whether the replicas should be rack aware or not.
   */
  public static void populate(ClusterModel cluster,
                              Map<ClusterProperty, Number> properties,
                              TestConstants.Distribution replicaDistribution,
                              boolean rackAware) {
    // Sanity checks.
    int numBrokers = cluster.brokers().size();
    if (properties.get(ClusterProperty.MEAN_NW_IN).doubleValue() < 0 ||
        properties.get(ClusterProperty.MEAN_NW_OUT).doubleValue() < 0 ||
        properties.get(ClusterProperty.MEAN_DISK).doubleValue() < 0 ||
        properties.get(ClusterProperty.MEAN_CPU).doubleValue() < 0 ||
        properties.get(ClusterProperty.NUM_DEAD_BROKERS).intValue() < 0 ||
        properties.get(ClusterProperty.NUM_TOPICS).intValue() <= 0 ||
        properties.get(ClusterProperty.MIN_REPLICATION).intValue() > properties.get(ClusterProperty.MAX_REPLICATION)
            .intValue() ||
        properties.get(ClusterProperty.MAX_REPLICATION).intValue() > numBrokers ||
        properties.get(ClusterProperty.NUM_TOPICS).intValue() > properties.get(ClusterProperty.NUM_REPLICAS).intValue() ||
        (properties.get(ClusterProperty.MIN_REPLICATION).intValue() == properties.get(ClusterProperty.MAX_REPLICATION).intValue() &&
            properties.get(ClusterProperty.NUM_REPLICAS).intValue() % properties.get(ClusterProperty.MIN_REPLICATION).intValue() != 0)) {
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
          } else { // Exponential.
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
          MetricValues metricValues = new MetricValues(1);
          metricValues.set(0, exponentialRandom(properties.get(ClusterProperty.MEAN_CPU).doubleValue() * topicPopularity,
                                                randomByResource.get(Resource.CPU)));
          aggregatedMetricValues.add(KafkaMetricDef.resourceToMetricId(Resource.CPU), metricValues);

          metricValues = new MetricValues(1);
          metricValues.set(0, exponentialRandom(properties.get(ClusterProperty.MEAN_NW_IN).doubleValue() * topicPopularity,
                                                randomByResource.get(Resource.NW_IN)));
          aggregatedMetricValues.add(KafkaMetricDef.resourceToMetricId(Resource.NW_IN), metricValues);

          metricValues = new MetricValues(1);
          metricValues.set(0, exponentialRandom(properties.get(ClusterProperty.MEAN_DISK).doubleValue() * topicPopularity,
                                                randomByResource.get(Resource.DISK)));
          aggregatedMetricValues.add(KafkaMetricDef.resourceToMetricId(Resource.DISK), metricValues);

          if (j == 1) {
            metricValues = new MetricValues(1);
            metricValues.set(0, exponentialRandom(properties.get(ClusterProperty.MEAN_NW_OUT).doubleValue() * topicPopularity,
                                                  randomByResource.get(Resource.NW_OUT)));
            aggregatedMetricValues.add(KafkaMetricDef.resourceToMetricId(Resource.NW_OUT), metricValues);
            cluster.createReplica(cluster.broker(randomBrokerId).rack().id(), randomBrokerId, pInfo, j - 1, true);
          } else {
            metricValues = new MetricValues(1);
            metricValues.set(0, 0.0);
            aggregatedMetricValues.add(KafkaMetricDef.resourceToMetricId(Resource.NW_OUT), metricValues);
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
      }
    }
    // Mark dead brokers.
    for (int i = 0; i < properties.get(ClusterProperty.NUM_DEAD_BROKERS).intValue(); i++) {
      cluster.setBrokerState(i, Broker.State.DEAD);
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
   * A helper class for random cluster generator to keep track of topic related metadata including the name,
   * replication factor, and number of leaders of the topic.
   */
  private static class TopicMetadata {
    private final String _topic;
    private int _replicationFactor;
    private int _numTopicLeaders;

    TopicMetadata(int topicId) {
      _topic = "T" + Integer.toString(topicId);
      _replicationFactor = 1;
      _numTopicLeaders = 1;
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
