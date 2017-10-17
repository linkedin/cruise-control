/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;

import java.util.HashMap;
import java.util.Map;


public class ClusterModelStats {
  private final Map<Statistic, Map<Resource, Double>> _resourceUtilizationStats;
  private final Map<Statistic, Double> _potentialNwOutUtilizationStats;
  private final Map<Statistic, Number> _replicaStats;
  private final Map<Statistic, Number> _topicReplicaStats;
  private int _numBrokers;
  private int _numReplicasInCluster;
  private int _numTopics;
  private Map<Resource, Integer> _numBalancedBrokersByResource;
  private int _numBrokersUnderPotentialNwOut;
  private BalancingConstraint _balancingConstraint;
  private double[][] _utilizationMatrix;
  private int _numSnapshotWindows;
  private double _monitoredPartitionsPercentage;

  /**
   * Constructor for analysis stats.
   */
  ClusterModelStats() {
    _resourceUtilizationStats = new HashMap<>();
    _potentialNwOutUtilizationStats = new HashMap<>();
    _replicaStats = new HashMap<>();
    _topicReplicaStats = new HashMap<>();
    _numBrokers = 0;
    _numReplicasInCluster = 0;
    _numTopics = 0;
    _numBrokersUnderPotentialNwOut = 0;
    _numBalancedBrokersByResource = new HashMap<>();
  }

  /**
   * Populate the analysis stats with this cluster and given balancing constraint.
   *
   * @param clusterModel        The state of the cluster.
   * @param balancingConstraint Balancing constraint.
   * @return Analysis stats with this cluster and given balancing constraint.
   */
  ClusterModelStats populate(ClusterModel clusterModel, BalancingConstraint balancingConstraint)
      throws ModelInputException {
    _numBrokers = clusterModel.brokers().size();
    _numTopics = clusterModel.topics().size();
    _balancingConstraint = balancingConstraint;
    utilizationForResources(clusterModel);
    utilizationForPotentialNwOut(clusterModel);
    numForReplicas(clusterModel);
    numForAvgTopicReplicas(clusterModel);
    _utilizationMatrix = clusterModel.utilizationMatrix();
    _numSnapshotWindows = clusterModel.load().numSnapshots();
    _monitoredPartitionsPercentage = clusterModel.monitoredPartitionsPercentage();
    return this;
  }

  /**
   * Get resource utilization stats for the cluster instance that the object was populated with.
   */
  public Map<Statistic, Map<Resource, Double>> resourceUtilizationStats() {
    return _resourceUtilizationStats;
  }

  /**
   * Get outbound network utilization stats for the cluster instance that the object was populated with.
   */
  public Map<Statistic, Double> potentialNwOutUtilizationStats() {
    return _potentialNwOutUtilizationStats;
  }

  /**
   * Get replica stats for the cluster instance that the object was populated with.
   */
  public Map<Statistic, Number> replicaStats() {
    return _replicaStats;
  }

  /**
   * Get topic replica stats for the cluster instance that the object was populated with.
   */
  public Map<Statistic, Number> topicReplicaStats() {
    return _topicReplicaStats;
  }

  /**
   * Get number of brokers for the cluster instance that the object was populated with.
   */
  public int numBrokers() {
    return _numBrokers;
  }

  /**
   * Get number of replicas for the cluster instance that the object was populated with.
   */
  public int numReplicasInCluster() {
    return _numReplicasInCluster;
  }

  /**
   * Get number of topics for the cluster instance that the object was populated with.
   */
  public int numTopics() {
    return _numTopics;
  }

  /**
   * Get number of balanced brokers by resource for the cluster instance that the object was populated with.
   */
  public Map<Resource, Integer> numBalancedBrokersByResource() {
    return _numBalancedBrokersByResource;
  }

  /**
   * Get number of brokers under potential nw out for the cluster instance that the object was populated with.
   */
  public int numBrokersUnderPotentialNwOut() {
    return _numBrokersUnderPotentialNwOut;
  }

  /**
   * This is the utilization matrix generated from {@link ClusterModel#utilizationMatrix()}.
   * @return non-null if populate has been called else this may return null.
   */
  public double[][] utilizationMatrix() {
    return _utilizationMatrix;
  }

  /**
   * Get the monitored partition percentage of this cluster model;
   */
  public double monitoredPartitionsPercentage() {
    return _monitoredPartitionsPercentage;
  }

  /**
   * Get the number of snapshot windows used by this cluster model;
   */
  public int numSnapshotWindows() {
    return _numSnapshotWindows;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("brokers:%d replicas:%d topics:%d%n",
                            numBrokers(),
                            numReplicasInCluster(),
                            numTopics()));
    for (Statistic stat : Statistic.values()) {
      sb.append(String.format("%s:{", stat));
      for (Resource resource : Resource.values()) {
        sb.append(String.format("%s:%12.3f ", resource, resourceUtilizationStats().get(stat).get(resource)));
      }
      sb.append(String.format("potentialNwOut:%12.3f replicas:%s topicReplicas:%s}%n",
                              potentialNwOutUtilizationStats().get(stat), replicaStats().get(stat),
                              topicReplicaStats().get(stat)));
    }
    return sb.substring(0, sb.length() - 2);
  }

  /**
   * Generate statistics of utilization for resources in the given cluster.
   *
   * @param clusterModel The state of the cluster.
   */
  private void utilizationForResources(ClusterModel clusterModel) {
    // Average, maximum, and standard deviation of utilization by resource.
    Map<Resource, Double> avgUtilizationByResource = new HashMap<>();
    Map<Resource, Double> maxUtilizationByResource = new HashMap<>();
    Map<Resource, Double> minUtilizationByResource = new HashMap<>();
    Map<Resource, Double> stDevUtilizationByResource = new HashMap<>();
    for (Resource resource : Resource.values()) {
      double balanceUpperThreshold = (clusterModel.load().expectedUtilizationFor(resource) / clusterModel.capacityFor(resource))
          * _balancingConstraint.balancePercentage(resource);
      double balanceLowerThreshold = (clusterModel.load().expectedUtilizationFor(resource) / clusterModel.capacityFor(resource))
          * Math.max(0, (2 - _balancingConstraint.balancePercentage(resource)));
      // Average utilization for the resource.
      double avgUtilization = clusterModel.load().expectedUtilizationFor(resource) / _numBrokers;
      avgUtilizationByResource.put(resource, avgUtilization);

      // Maximum, minimum, and standard deviation utilization for the resource.
      double hottestBrokerUtilization = 0.0;
      double coldestBrokerUtilization = Double.MAX_VALUE;
      double variance = 0.0;
      int numBalancedBrokers = 0;
      for (Broker broker : clusterModel.brokers()) {
        double utilization = resource.isHostResource() ?
            broker.host().load().expectedUtilizationFor(resource) : broker.load().expectedUtilizationFor(resource);
        double balanceUpperLimit = resource.isHostResource() ?
            broker.host().capacityFor(resource) * balanceUpperThreshold : broker.capacityFor(resource) * balanceUpperThreshold;
        double balanceLowerLimit = resource.isHostResource() ?
            broker.host().capacityFor(resource) * balanceLowerThreshold : broker.capacityFor(resource) * balanceLowerThreshold;
        if (utilization <= balanceUpperLimit && utilization >= balanceLowerLimit) {
          numBalancedBrokers++;
        }
        double brokerUtilization = broker.load().expectedUtilizationFor(resource);
        hottestBrokerUtilization = Math.max(hottestBrokerUtilization, brokerUtilization);
        coldestBrokerUtilization = Math.min(coldestBrokerUtilization, brokerUtilization);
        variance += (Math.pow(brokerUtilization - avgUtilization, 2) / _numBrokers);
      }
      _numBalancedBrokersByResource.put(resource, numBalancedBrokers);
      maxUtilizationByResource.put(resource, hottestBrokerUtilization);
      minUtilizationByResource.put(resource, coldestBrokerUtilization);
      stDevUtilizationByResource.put(resource, Math.sqrt(variance));
    }

    _resourceUtilizationStats.put(Statistic.AVG, avgUtilizationByResource);
    _resourceUtilizationStats.put(Statistic.MAX, maxUtilizationByResource);
    _resourceUtilizationStats.put(Statistic.MIN, minUtilizationByResource);
    _resourceUtilizationStats.put(Statistic.ST_DEV, stDevUtilizationByResource);
  }

  /**
   * Generate statistics of utilization for potential network outbound utilization in the given cluster.
   *
   * @param clusterModel The state of the cluster.
   */
  private void utilizationForPotentialNwOut(ClusterModel clusterModel) {
    // Average, minimum, and maximum: network outbound utilization and replicas in brokers.
    double potentialNwOutInCluster = 0.0;
    double maxPotentialNwOut = 0.0;
    double minPotentialNwOut = Double.MAX_VALUE;
    for (Broker broker : clusterModel.brokers()) {
      double capacityLimit = broker.capacityFor(Resource.NW_OUT) * _balancingConstraint.capacityThreshold(Resource.NW_OUT);

      if (clusterModel.potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT) <= capacityLimit) {
        _numBrokersUnderPotentialNwOut++;
      }
      double brokerUtilization = clusterModel.potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT);
      maxPotentialNwOut = Math.max(maxPotentialNwOut, brokerUtilization);
      minPotentialNwOut = Math.min(minPotentialNwOut, brokerUtilization);
      potentialNwOutInCluster += brokerUtilization;
    }
    double avgPotentialNwOut = potentialNwOutInCluster / _numBrokers;

    // Standard deviation of network outbound utilization.
    double varianceForPotentialNwOut = 0.0;
    for (Broker broker : clusterModel.brokers()) {
      double brokerUtilization = clusterModel.potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT);
      varianceForPotentialNwOut += (Math.pow(brokerUtilization - avgPotentialNwOut, 2) / _numBrokers);
    }

    _potentialNwOutUtilizationStats.put(Statistic.AVG, avgPotentialNwOut);
    _potentialNwOutUtilizationStats.put(Statistic.MAX, maxPotentialNwOut);
    _potentialNwOutUtilizationStats.put(Statistic.MIN, minPotentialNwOut);
    _potentialNwOutUtilizationStats.put(Statistic.ST_DEV, Math.sqrt(varianceForPotentialNwOut));
  }

  /**
   * Generate statistics for replicas in the given cluster.
   *
   * @param clusterModel The state of the cluster.
   */
  private void numForReplicas(ClusterModel clusterModel)
      throws ModelInputException {
    // Average, minimum, and maximum number of replicas in brokers.
    int maxReplicasInBroker = 0;
    int minReplicasInBroker = Integer.MAX_VALUE;
    for (Broker broker : clusterModel.brokers()) {
      int numReplicasInBroker = broker.replicas().size();
      _numReplicasInCluster += numReplicasInBroker;
      maxReplicasInBroker = Math.max(maxReplicasInBroker, numReplicasInBroker);
      minReplicasInBroker = Math.min(minReplicasInBroker, numReplicasInBroker);
    }
    double avgReplicas = ((double) _numReplicasInCluster) / _numBrokers;

    // Standard deviation of replicas in brokers.
    double varianceForReplicas = 0.0;
    for (Broker broker : clusterModel.brokers()) {
      varianceForReplicas += (Math.pow((double) broker.replicas().size() - avgReplicas, 2) / _numBrokers);
    }

    _replicaStats.put(Statistic.AVG, avgReplicas);
    _replicaStats.put(Statistic.MAX, maxReplicasInBroker);
    _replicaStats.put(Statistic.MIN, minReplicasInBroker);
    _replicaStats.put(Statistic.ST_DEV, Math.sqrt(varianceForReplicas));
  }

  /**
   * Generate statistics for topic replicas in the given cluster.
   *
   * @param clusterModel The state of the cluster.
   */
  private void numForAvgTopicReplicas(ClusterModel clusterModel) throws ModelInputException {
    _topicReplicaStats.put(Statistic.AVG, 0.0);
    _topicReplicaStats.put(Statistic.MAX, 0);
    _topicReplicaStats.put(Statistic.MIN, Integer.MAX_VALUE);
    _topicReplicaStats.put(Statistic.ST_DEV, 0.0);
    for (String topic : clusterModel.topics()) {
      int maxTopicReplicasInBroker = 0;
      int minTopicReplicasInBroker = Integer.MAX_VALUE;
      for (Broker broker : clusterModel.brokers()) {
        int numTopicReplicasInBroker = broker.replicasOfTopicInBroker(topic).size();
        maxTopicReplicasInBroker = Math.max(maxTopicReplicasInBroker, numTopicReplicasInBroker);
        minTopicReplicasInBroker = Math.min(minTopicReplicasInBroker, numTopicReplicasInBroker);
      }
      double avgTopicReplicas = ((double) clusterModel.numTopicReplicas(topic)) / _numBrokers;

      // Standard deviation of replicas in brokers.
      double variance = 0.0;
      for (Broker broker : clusterModel.brokers()) {
        variance += (Math.pow(broker.replicasOfTopicInBroker(topic).size() - avgTopicReplicas, 2)
            / (double) _numBrokers);
      }

      _topicReplicaStats.put(Statistic.AVG, _topicReplicaStats.get(Statistic.AVG).doubleValue() + avgTopicReplicas);
      _topicReplicaStats.put(Statistic.MAX,
          Math.max(_topicReplicaStats.get(Statistic.MAX).intValue(), maxTopicReplicasInBroker));
      _topicReplicaStats.put(Statistic.MIN,
          Math.min(_topicReplicaStats.get(Statistic.MIN).intValue(), minTopicReplicasInBroker));
      _topicReplicaStats.put(Statistic.ST_DEV, (Double) _topicReplicaStats.get(Statistic.ST_DEV) + Math.sqrt(variance));
    }

    _topicReplicaStats.put(Statistic.AVG, _topicReplicaStats.get(Statistic.AVG).doubleValue() / _numTopics);
    _topicReplicaStats.put(Statistic.ST_DEV, _topicReplicaStats.get(Statistic.ST_DEV).doubleValue() / _numTopics);
  }
}
