/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;

import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.kafka.common.TopicPartition;

/**
 * A class that holds the information of the cluster, including topology, liveness and load for racks, brokers and
 * replicas. A cluster object is created either by a load balance request or a self healing request. It is used as
 * the input of the analyzer to generate the proposals for load rebalance.
 */
public class ClusterModel implements Serializable {
  private static final long serialVersionUID = -6840253566423285966L;

  private final ModelGeneration _generation;
  private final Map<String, Rack> _racksById;
  private final Map<Integer, Rack> _brokerIdToRack;
  private final Map<TopicPartition, Partition> _partitionsByTopicPartition;
  private final Set<Replica> _selfHealingEligibleReplicas;
  private final SortedSet<Broker> _newBrokers;
  private final Set<Broker> _healthyBrokers;
  private final SortedSet<Broker> _deadBrokes;
  private final SortedSet<Broker> _brokers;
  private final double _monitoredPartitionsPercentage;
  private final double[] _clusterCapacity;
  private Load _load;
  // An integer to keep track of the maximum replication factor that a partition was ever created with.
  private int _maxReplicationFactor;
  // The replication factor that each topic in the cluster created with ().
  private Map<String, Integer> _replicationFactorByTopic;
  private Map<Integer, Load> _potentialLeadershipLoadByBrokerId;
  private int _unknownHostId;
  private Map<Integer, String> _capacityEstimationInfoByBrokerId;

  /**
   * Constructor for the cluster class. It creates data structures to hold a list of racks, a map for partitions by
   * topic partition, topic replica collocation by topic.
   */
  public ClusterModel(ModelGeneration generation, double monitoredPartitionsPercentage) {
    _generation = generation;
    _racksById = new HashMap<>();
    _brokerIdToRack = new HashMap<>();
    _partitionsByTopicPartition = new HashMap<>();
    // Replicas are added/removed only when broker alive status is set via setState(). Replica contents are
    // automatically updated in case of replica or leadership relocation.
    _selfHealingEligibleReplicas = new HashSet<>();
    // A sorted set of newly added brokers
    _newBrokers = new TreeSet<>();
    // A set of healthy brokers
    _healthyBrokers = new HashSet<>();
    // A set of all brokers
    _brokers = new TreeSet<>();
    // A set of dead brokers
    _deadBrokes = new TreeSet<>();
    // Initially cluster does not contain any load.
    _load = new Load();
    _clusterCapacity = new double[Resource.cachedValues().size()];
    _maxReplicationFactor = 1;
    _replicationFactorByTopic = new HashMap<>();
    _potentialLeadershipLoadByBrokerId = new HashMap<>();
    _monitoredPartitionsPercentage = monitoredPartitionsPercentage;
    _unknownHostId = 0;
    _capacityEstimationInfoByBrokerId = new HashMap<>();
  }

  /**
   * get the metadata generation for this cluster model.
   */
  public ModelGeneration generation() {
    return _generation;
  }

  /**
   * Get the coverage of this cluster model. This shows how representative the cluster is.
   */
  public double monitoredPartitionsPercentage() {
    return _monitoredPartitionsPercentage;
  }

  /**
   * Populate the analysis stats with this cluster and given balancing constraint.
   *
   * @param balancingConstraint Balancing constraint.
   * @return Analysis stats with this cluster and given balancing constraint.
   */
  public ClusterModelStats getClusterStats(BalancingConstraint balancingConstraint) {
    return (new ClusterModelStats()).populate(this, balancingConstraint);
  }

  /**
   * Get the rack with the rack id if it is found in the cluster; null otherwise.
   */
  public Rack rack(String rackId) {
    return _racksById.get(rackId);
  }

  /**
   * Get the distribution of replicas in the cluster at the point of call. Replica distribution is represented by the
   * map: topic-partition -&gt; broker-id-of-replicas. broker-id-of-replicas[0] represents the leader's broker id.
   *
   * @return The replica distribution of leader and follower replicas in the cluster at the point of call.
   */
  public Map<TopicPartition, List<Integer>> getReplicaDistribution() {
    Map<TopicPartition, List<Integer>> replicaDistribution = new HashMap<>();

    for (Map.Entry<TopicPartition, Partition> entry : _partitionsByTopicPartition.entrySet()) {
      TopicPartition tp = entry.getKey();
      Partition partition = entry.getValue();
      List<Integer> brokerIds = new ArrayList<>();
      partition.replicas().forEach(r -> brokerIds.add(r.broker().id()));
      // Add distribution of replicas in the partition.
      replicaDistribution.put(tp, brokerIds);
    }

    return replicaDistribution;
  }

  /**
   * Get leader broker ids for each partition.
   */
  public Map<TopicPartition, Integer> getLeaderDistribution() {
    Map<TopicPartition, Integer> leaders = new HashMap<>();
    for (Map.Entry<TopicPartition, Partition> entry : _partitionsByTopicPartition.entrySet()) {
      leaders.put(entry.getKey(), entry.getValue().leader().broker().id());
    }
    return leaders;
  }

  /**
   * Get replicas eligible for self-healing.
   */
  public Set<Replica> selfHealingEligibleReplicas() {
    return _selfHealingEligibleReplicas;
  }

  /**
   * Get the recent cluster load information.
   */
  public Load load() {
    return _load;
  }

  /**
   * Get the leadership load for given broker id. Leadership load is the accumulated outbound network load for leader
   * of each replica in a broker.  This is the hypothetical maximum that would be realized if the specified broker
   * became the leader of all the replicas it currently owns.
   *
   * @param brokerId Broker id.
   * @return The leadership load for broker.
   */
  public Load potentialLeadershipLoadFor(Integer brokerId) {
    return _potentialLeadershipLoadByBrokerId.get(brokerId);
  }

  /**
   * Get the maximum replication factor of a replica that was added to the cluster before.
   */
  public int maxReplicationFactor() {
    return _maxReplicationFactor;
  }
  /**
   * Get the replication factor that each topic in the cluster created with.
   */
  public Map<String, Integer> replicationFactorByTopic() {
    return _replicationFactorByTopic;
  }

  /**
   * Get partition of the given replica.
   *
   * @param tp Topic partition of the replica for which the partition is requested.
   * @return Partition of the given replica.
   */
  public Partition partition(TopicPartition tp) {
    return _partitionsByTopicPartition.get(tp);
  }

  /**
   * Get a map of partitions by topic names.
   */
  public SortedMap<String, List<Partition>> getPartitionsByTopic() {
    SortedMap<String, List<Partition>> partitionsByTopic = new TreeMap<>();
    for (String topicName: topics()) {
      partitionsByTopic.put(topicName, new ArrayList<>());
    }
    for (Map.Entry<TopicPartition, Partition> entry: _partitionsByTopicPartition.entrySet()) {
      partitionsByTopic.get(entry.getKey().topic()).add(entry.getValue());
    }
    return partitionsByTopic;
  }

  /**
   * Get all the leader replicas in the cluster.
   */
  public Set<Replica> leaderReplicas() {
    return _partitionsByTopicPartition.values().stream().map(Partition::leader).collect(Collectors.toSet());
  }

  /**
   * Set broker alive status. If broker is not alive, add its replicas to self healing eligible replicas, if broker
   * alive status is set to true, remove its replicas from self healing eligible replicas.
   *
   * @param brokerId    Id of the broker for which the alive status is set.
   * @param newState True if alive, false otherwise.
   */
  public void setBrokerState(int brokerId, Broker.State newState) {
    Broker broker = broker(brokerId);
    if (broker == null) {
      throw new IllegalArgumentException("Broker " + brokerId + " does not exist.");
    }
    // We need to go through rack so all the cached capacity will be updated.
    broker.rack().setBrokerState(brokerId, newState);
    refreshCapacity();
    switch (newState) {
      case DEAD:
        _selfHealingEligibleReplicas.addAll(broker.replicas());
        _healthyBrokers.remove(broker);
        _deadBrokes.add(broker);
        break;
      case NEW:
        _newBrokers.add(broker);
        // fall through to remove the replicas from selfHealingEligibleReplicas
      case DEMOTED:
        // As of now we still treat demoted brokers as alive brokers.
      case ALIVE:
        _selfHealingEligibleReplicas.removeAll(broker.replicas());
        _healthyBrokers.add(broker);
        _deadBrokes.remove(broker);
        break;
      default:
        throw new IllegalArgumentException("Illegal broker state " + newState + " is provided.");
    }
  }

  /**
   * (1) Remove the replica from the source broker,
   * (2) Set the broker of the removed replica as the destination broker,
   * (3) Add this replica to the destination broker.
   * * There is no need to make any modifications to _partitionsByTopicPartition because even after the move,
   * partitions will contain the same replicas.
   *
   * @param tp      Partition Info of the replica to be relocated.
   * @param sourceBrokerId      Source broker id.
   * @param destinationBrokerId Destination broker id.
   */
  public void relocateReplica(TopicPartition tp, int sourceBrokerId, int destinationBrokerId) {
    // Removes the replica and related load from the source broker / source rack / cluster.
    Replica replica = removeReplica(sourceBrokerId, tp);
    if (replica == null) {
      throw new IllegalArgumentException("Replica is not in the cluster.");
    }
    // Updates the broker of the removed replica with destination broker.
    replica.setBroker(broker(destinationBrokerId));

    // Add this replica and related load to the destination broker / destination rack / cluster.
    replica.broker().rack().addReplica(replica);
    _load.addLoad(replica.load());
    // Add leadership load to the destination replica.
    _potentialLeadershipLoadByBrokerId.get(destinationBrokerId).addLoad(partition(tp).leader().load());
  }

  /**
   * (1) Removes leadership from source replica.
   * (2) Adds this leadership to the destination replica.
   * (3) Transfers the whole outbound network and a fraction of CPU load of source replica to the destination replica.
   * (4) Updates the leader and list of followers of the partition.
   *
   * @param tp      Topic partition of this replica.
   * @param sourceBrokerId      Source broker id.
   * @param destinationBrokerId Destination broker id.
   * @return True if relocation is successful, false otherwise.
   */
  public boolean relocateLeadership(TopicPartition tp, int sourceBrokerId, int destinationBrokerId) {
    // Sanity check to see if the source replica is the leader.
    Replica sourceReplica = _partitionsByTopicPartition.get(tp).replica(sourceBrokerId);
    if (!sourceReplica.isLeader()) {
      return false;
    }
    // Sanity check to see if the destination replica is a follower.
    Replica destinationReplica = _partitionsByTopicPartition.get(tp).replica(destinationBrokerId);
    if (destinationReplica.isLeader()) {
      throw new IllegalArgumentException("Cannot relocate leadership of partition " + tp + "from broker "
                                         + sourceBrokerId + " to broker " + destinationBrokerId
                                         + " because the destination replica is a leader.");
    }

    // Transfer the leadership load (whole outbound network and a fraction of CPU load) of source replica to the
    // destination replica.
    // (1) Remove and get the outbound network load and a fraction of CPU load associated with leadership from the
    // given replica.
    // (2) Add the outbound network load and CPU load associated with leadership to the given replica.
    //
    // Remove the load from the source rack.
    Rack rack = broker(sourceBrokerId).rack();
    AggregatedMetricValues leadershipLoadDelta = rack.makeFollower(sourceBrokerId, tp);
    // Add the load to the destination rack.
    rack = broker(destinationBrokerId).rack();
    rack.makeLeader(destinationBrokerId, tp, leadershipLoadDelta);

    // Update the leader and list of followers of the partition.
    Partition partition = _partitionsByTopicPartition.get(tp);
    partition.relocateLeadership(destinationReplica);

    return true;
  }

  /**
   * Get healthy brokers in the cluster.
   */
  public Set<Broker> healthyBrokers() {
    return _healthyBrokers;
  }

  /**
   * Get the dead brokers in the cluster.
   */
  public SortedSet<Broker> deadBrokers() {
    return new TreeSet<>(_deadBrokes);
  }

  /**
   * @return Capacity estimation info by broker id for which there has been an estimation.
   */
  public Map<Integer, String> capacityEstimationInfoByBrokerId() {
    return Collections.unmodifiableMap(_capacityEstimationInfoByBrokerId);
  }

  /**
   * Get the demoted brokers in the cluster.
   */
  public SortedSet<Broker> demotedBrokers() {
    SortedSet<Broker> demotedBrokers = new TreeSet<>();
    for (Rack rack : _racksById.values()) {
      rack.brokers().forEach(b -> {
        if (b.isDemoted()) {
          demotedBrokers.add(b);
        }
      });
    }
    return demotedBrokers;
  }

  /**
   * Get the set of new brokers.
   */
  public SortedSet<Broker> newBrokers() {
    return _newBrokers;
  }

  /**
   * Checks if cluster has at least one alive rack. If none of the racks are alive, cluster is considered dead.
   */
  public boolean isClusterAlive() {
    for (Rack rack : _racksById.values()) {
      if (rack.isRackAlive()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Clear the content of monitoring data at each replica in the cluster.
   * Typically, if a change is detected in topology, this method is called to clear the monitoring data collected with
   * the old topology.
   */
  public void clearLoad() {
    _racksById.values().forEach(Rack::clearLoad);
    _load.clearLoad();
  }

  /**
   * Remove and get removed replica from the cluster.
   *
   * @param brokerId       Id of the broker containing the partition.
   * @param tp Topic partition of the replica to be removed.
   * @return The requested replica if the id exists in the rack and the partition is found in the broker, null
   * otherwise.
   */
  public Replica removeReplica(int brokerId, TopicPartition tp) {
    for (Rack rack : _racksById.values()) {
      // Remove the replica and the associated load from the rack that it resides in.
      Replica removedReplica = rack.removeReplica(brokerId, tp);
      if (removedReplica != null) {
        // Remove the load of the removed replica from the recent load of the cluster.
        _load.subtractLoad(removedReplica.load());
        _potentialLeadershipLoadByBrokerId.get(brokerId).subtractLoad(partition(tp).leader().load());
        // Return the removed replica.
        return removedReplica;
      }
    }
    return null;
  }

  /**
   * Get the set of brokers in the cluster.
   */
  public SortedSet<Broker> brokers() {
    return new TreeSet<>(_brokers);
  }

  /**
   * Get the requested broker in the cluster.
   *
   * @param brokerId Id of the requested broker.
   * @return Requested broker if it is in the cluster, null otherwise.
   */
  public Broker broker(int brokerId) {
    Rack rack = _brokerIdToRack.get(brokerId);
    return rack == null ? null : rack.broker(brokerId);
  }

  /**
   * Ask the cluster model to keep track of the replicas sorted with the given score function.
   *
   * It is recommended to use the functions from {@link ReplicaSortFunctionFactory} so the functions can be maintained
   * in a single place.
   *
   * The sorted replica will only be updated in the following cases:
   * 1. A replica is added to or removed from a broker
   * 2. A replica's role has changed from leader to follower, and vice versa.
   *
   * The sorted replicas are named using the given sortName, and can be accessed using
   * {@link Broker#trackedSortedReplicas(String)}. If the sorted replicas are no longer needed,
   * {@link #untrackSortedReplicas(String)} to release memory.
   *
   * @param sortName the name of the sorted replicas.
   * @param scoreFunction the score function to sort the replicas with the same priority.
   * @see SortedReplicas
   */
  public void trackSortedReplicas(String sortName, Function<Replica, Double> scoreFunction) {
    trackSortedReplicas(sortName, null, scoreFunction);
  }

  /**
   * Ask the cluster model to keep track of the replicas sorted with the given priority function and score function.
   *
   * It is recommended to use the functions from {@link ReplicaSortFunctionFactory} so the functions can be maintained
   * in a single place.
   *
   * The sort will first use the priority function then the score function. The priority function allows the
   * caller to prioritize a certain type of replicas, e.g immigrant replicas.
   *
   * The sorted replica will only be updated in the following cases:
   * 1. A replica is added to or removed from abroker
   * 2. A replica's role has changed from leader to follower, and vice versa.
   *
   * The sorted replicas are named using the given sortName, and can be accessed using
   * {@link Broker#trackedSortedReplicas(String)}. If the sorted replicas are no longer needed,
   * {@link #untrackSortedReplicas(String)} to release memory.
   *
   * @param sortName the name of the sorted replicas.
   * @param priorityFunc the priority function to sort the replicas
   * @param scoreFunc the score function to sort the replicas with the same priority.
   * @see SortedReplicas
   */
  public void trackSortedReplicas(String sortName,
                                  Function<Replica, Integer> priorityFunc,
                                  Function<Replica, Double> scoreFunc) {
    trackSortedReplicas(sortName, null, priorityFunc, scoreFunc);
  }

  /**
   * Ask the cluster model to keep track of the replicas sorted with the given priority function and score function.
   *
   * The sort will first use the priority function then the score function. The priority function allows the
   * caller to prioritize a certain type of replicas, e.g immigrant replicas. The selection function determines
   * which replicas to be included in the sorted replicas.
   *
   * It is recommended to use the functions from {@link ReplicaSortFunctionFactory} so the functions can be maintained
   * in a single place.
   *
   * The sorted replica will only be updated in the following cases:
   * 1. A replica is added to or removed from a broker
   * 2. A replica's role has changed from leader to follower, and vice versa.
   *
   * The sorted replicas are named using the given sortName, and can be accessed using
   * {@link Broker#trackedSortedReplicas(String)}. If the sorted replicas are no longer needed,
   * {@link #untrackSortedReplicas(String)} to release memory.
   *
   * @param sortName the name of the sorted replicas.
   * @param selectionFunc the selection function to decide which replicas to include in the sort.
   * @param priorityFunc the priority function to sort the replicas
   * @param scoreFunc the score function to sort the replicas with the same priority.
   * @see SortedReplicas
   */
  public void trackSortedReplicas(String sortName,
                                  Function<Replica, Boolean> selectionFunc,
                                  Function<Replica, Integer> priorityFunc,
                                  Function<Replica, Double> scoreFunc) {
    _brokers.forEach(b -> b.trackSortedReplicas(sortName, selectionFunc, priorityFunc, scoreFunc));
  }

  /**
   * Untrack the sorted replicas with the given name to release memory.
   *
   * @param sortName the name of the sorted replicas.
   */
  public void untrackSortedReplicas(String sortName) {
    _brokers.forEach(b -> b.untrackSortedReplicas(sortName));
  }

  /**
   * Clear the content and structure of the cluster.
   */
  public void clear() {
    _racksById.clear();
    _partitionsByTopicPartition.clear();
    _load.clearLoad();
    _maxReplicationFactor = 1;
    _replicationFactorByTopic.clear();
    _capacityEstimationInfoByBrokerId.clear();
  }

  /**
   * Get number of healthy racks in the cluster.
   */
  public int numHealthyRacks() {
    int numHealthyRacks = 0;
    for (Rack rack : _racksById.values()) {
      if (rack.isRackAlive()) {
        numHealthyRacks++;
      }
    }
    return numHealthyRacks;
  }

  /**
   * Get the number of replicas with the given topic name in cluster.
   *
   * @param topic Name of the topic for which the number of replicas in cluster will be counted.
   * @return Number of replicas with the given topic name in cluster.
   */
  public int numTopicReplicas(String topic) {
    int numTopicReplicas = 0;

    for (Rack rack : _racksById.values()) {
      numTopicReplicas += rack.numTopicReplicas(topic);
    }
    return numTopicReplicas;
  }

  /**
   * Get topics in the cluster.
   */
  public Set<String> topics() {
    Set<String> topics = new HashSet<>();

    for (Rack rack : _racksById.values()) {
      topics.addAll(rack.topics());
    }
    return topics;
  }

  /**
   * Get cluster capacity for the requested resource. Cluster capacity represents the total capacity of the live
   * brokers in the cluster for the requested resource.
   *
   * @param resource Resource for which the capacity will be provided.
   * @return Healthy cluster capacity of the resource.
   */
  public double capacityFor(Resource resource) {
    return _clusterCapacity[resource.id()];
  }

  /**
   * Set the load for the given replica. This method should be called only once for each replica.
   *
   * @param rackId         Rack id.
   * @param brokerId       Broker Id containing the replica with the given topic partition.
   * @param tp             Topic partition that identifies the replica in this broker.
   * @param metricValues   The load of the replica.
   * @param windows        The windows list of the aggregated metrics.
   */
  public void setReplicaLoad(String rackId,
                             int brokerId,
                             TopicPartition tp,
                             AggregatedMetricValues metricValues,
                             List<Long> windows) {
    // Sanity check for the attempts to push more than allowed number of snapshots having different times.
    if (!broker(brokerId).replica(tp).load().isEmpty()) {
      throw new IllegalStateException(String.format("The load for %s on broker %d, rack %s already has metric values.",
                                                    tp, brokerId, rackId));
    }

    Rack rack = rack(rackId);
    rack.setReplicaLoad(brokerId, tp, metricValues, windows);

    // Update the recent load of cluster.
    _load.addMetricValues(metricValues, windows);
    // If this snapshot belongs to leader, update leadership load.
    Replica leader = partition(tp).leader();
    if (leader != null && leader.broker().id() == brokerId) {
      // load must be updated for each broker containing a replica of the same partition.
      for (Replica replica : partition(tp).replicas()) {
        _potentialLeadershipLoadByBrokerId.get(replica.broker().id()).addMetricValues(metricValues, windows);
      }
    }
  }

  /**
   * If the rack or broker does not exist, create them with UNKNOWN host name. This allows handling
   * of cases where the information of a dead broker is no longer available.
   *
   * @param rackId         Rack id under which the replica will be created.
   * @param brokerId       Broker id under which the replica will be created.
   * @param brokerCapacityInfo The capacity information to use if the broker does not exist.
   */
  public void handleDeadBroker(String rackId, int brokerId, BrokerCapacityInfo brokerCapacityInfo) {
    if (rack(rackId) == null) {
      createRack(rackId);
    }
    if (broker(brokerId) == null) {
      createBroker(rackId, String.format("UNKNOWN_HOST-%d", _unknownHostId++), brokerId, brokerCapacityInfo);
    }
  }

  /**
   * Create a replica under given cluster/rack/broker. Add replica to rack and corresponding partition. Get the
   * created replica.
   *
   * @param rackId         Rack id under which the replica will be created.
   * @param brokerId       Broker id under which the replica will be created.
   * @param tp             Topic partition information of the replica.
   * @param index          The index of the replica in the replica list.
   * @param isLeader       True if the replica is a leader, false otherwise.
   * @return Created replica.
   */
  public Replica createReplica(String rackId, int brokerId, TopicPartition tp, int index, boolean isLeader) {
    Replica replica = new Replica(tp, broker(brokerId), isLeader);
    rack(rackId).addReplica(replica);

    // Add replica to its partition.
    if (!_partitionsByTopicPartition.containsKey(tp)) {
      // Partition has not been created before.
      _partitionsByTopicPartition.put(tp, new Partition(tp));
      _replicationFactorByTopic.putIfAbsent(tp.topic(), 1);
    }

    Partition partition = _partitionsByTopicPartition.get(tp);
    if (replica.isLeader()) {
      partition.addLeader(replica, index);
      return replica;
    }

    partition.addFollower(replica, index);
    // If leader of this follower was already created and load was pushed to it, add that load to the follower.
    Replica leaderReplica = partition(tp).leader();
    if (leaderReplica != null) {
      _potentialLeadershipLoadByBrokerId.get(brokerId).addLoad(leaderReplica.load());
    }

    // Keep track of the replication factor per topic.
    Integer replicationFactor = Math.max(_replicationFactorByTopic.get(tp.topic()), partition.followers().size() + 1);
    _replicationFactorByTopic.put(tp.topic(), replicationFactor);

    // Increment the maximum replication factor if the number of replicas of the partition is larger than the
    //  maximum replication factor of previously existing partitions.
    _maxReplicationFactor = Math.max(_maxReplicationFactor, replicationFactor);

    return replica;
  }

  /**
   * Create a broker under this cluster/rack and get the created broker.
   * Add the broker id and info to {@link #_capacityEstimationInfoByBrokerId} if the broker capacity has been estimated.
   *
   * @param rackId Id of the rack that the broker will be created in.
   * @param host The host of this broker
   * @param brokerId Id of the broker to be created.
   * @param brokerCapacityInfo Capacity information of the created broker.
   * @return Created broker.
   */
  public Broker createBroker(String rackId, String host, int brokerId, BrokerCapacityInfo brokerCapacityInfo) {
    _potentialLeadershipLoadByBrokerId.putIfAbsent(brokerId, new Load());
    Rack rack = rack(rackId);
    _brokerIdToRack.put(brokerId, rack);

    if (brokerCapacityInfo.isEstimated()) {
      _capacityEstimationInfoByBrokerId.put(brokerId, brokerCapacityInfo.estimationInfo());
    }
    Broker broker = rack.createBroker(brokerId, host, brokerCapacityInfo.capacity());
    _healthyBrokers.add(broker);
    _brokers.add(broker);
    refreshCapacity();
    return broker;
  }

  /**
   * Create a rack under this cluster.
   *
   * @param rackId Id of the rack to be created.
   * @return Created rack.
   */
  public Rack createRack(String rackId) {
    Rack rack = new Rack(rackId);
    return _racksById.putIfAbsent(rackId, rack);
  }

  /**
   * Get a list of sorted (in ascending order by resource) healthy brokers having utilization under:
   * (given utilization threshold) * (broker and/or host capacity (see {@link Resource#_isHostResource} and
   * {@link Resource#_isBrokerResource}). Utilization threshold might be any capacity constraint thresholds such as
   * balance or capacity.
   *
   * @param resource             Resource for which brokers will be sorted.
   * @param utilizationThreshold Utilization threshold for the given resource.
   * @return A list of sorted (in ascending order by resource) healthy brokers having utilization under:
   * (given utilization threshold) * (broker and/or host capacity).
   */
  public List<Broker> sortedHealthyBrokersUnderThreshold(Resource resource, double utilizationThreshold) {
    List<Broker> sortedTargetBrokersUnderCapacityLimit = healthyBrokersUnderThreshold(resource, utilizationThreshold);

    sortedTargetBrokersUnderCapacityLimit.sort((o1, o2) -> {
      Double expectedBrokerLoad1 = o1.load().expectedUtilizationFor(resource);
      Double expectedBrokerLoad2 = o2.load().expectedUtilizationFor(resource);
      // For host resource we first compare host util then look at the broker util -- even if a resource is a
      // host-resource, but not broker-resource.
      int hostComparison = 0;
      if (resource.isHostResource()) {
        Double expectedHostLoad1 = resource.isHostResource() ? o1.host().load().expectedUtilizationFor(resource) : 0.0;
        Double expectedHostLoad2 = resource.isHostResource() ? o2.host().load().expectedUtilizationFor(resource) : 0.0;
        hostComparison = Double.compare(expectedHostLoad1, expectedHostLoad2);
      }
      return hostComparison == 0 ? Double.compare(expectedBrokerLoad1, expectedBrokerLoad2) : hostComparison;
    });
    return sortedTargetBrokersUnderCapacityLimit;
  }

  public List<Broker> healthyBrokersUnderThreshold(Resource resource, double utilizationThreshold) {
    List<Broker> healthyBrokersUnderThreshold = new ArrayList<>();

    for (Broker healthyBroker : healthyBrokers()) {
      if (resource.isBrokerResource()) {
        double brokerCapacityLimit = healthyBroker.capacityFor(resource) * utilizationThreshold;
        double brokerUtilization = healthyBroker.load().expectedUtilizationFor(resource);
        if (brokerUtilization >= brokerCapacityLimit) {
          continue;
        }
      }
      if (resource.isHostResource()) {
        double hostCapacityLimit = healthyBroker.host().capacityFor(resource) * utilizationThreshold;
        double hostUtilization = healthyBroker.host().load().expectedUtilizationFor(resource);
        if (hostUtilization >= hostCapacityLimit) {
          continue;
        }
      }
      healthyBrokersUnderThreshold.add(healthyBroker);
    }
    return healthyBrokersUnderThreshold;
  }

  public List<Broker> healthyBrokersOverThreshold(Resource resource, double utilizationThreshold) {
    List<Broker> healthyBrokersOverThreshold = new ArrayList<>();

    for (Broker healthyBroker : healthyBrokers()) {
      if (resource.isBrokerResource()) {
        double brokerCapacityLimit = healthyBroker.capacityFor(resource) * utilizationThreshold;
        double brokerUtilization = healthyBroker.load().expectedUtilizationFor(resource);
        if (brokerUtilization <= brokerCapacityLimit) {
          continue;
        }
      }
      if (resource.isHostResource()) {
        double hostCapacityLimit = healthyBroker.host().capacityFor(resource) * utilizationThreshold;
        double hostUtilization = healthyBroker.host().load().expectedUtilizationFor(resource);
        if (hostUtilization <= hostCapacityLimit) {
          continue;
        }
      }
      healthyBrokersOverThreshold.add(healthyBroker);
    }
    return healthyBrokersOverThreshold;
  }

  /**
   * Sort replicas in ascending order of resource quantity present in the broker that they reside in terms of the
   * requested resource.
   *
   * @param replicas A list of replicas to be sorted by the amount of resources that their broker contains.
   * @param resource Resource for which the given replicas will be sorted.
   */
  public void sortReplicasInAscendingOrderByBrokerResourceUtilization(List<Replica> replicas, Resource resource) {
    replicas.sort((r1, r2) -> {
      Double expectedBrokerLoad1 = r1.broker().load().expectedUtilizationFor(resource);
      Double expectedBrokerLoad2 = r2.broker().load().expectedUtilizationFor(resource);
      int result = Double.compare(expectedBrokerLoad1, expectedBrokerLoad2);
      return result == 0 ? Integer.compare(r1.broker().id(), r2.broker().id()) : result;
    });
  }

  /**
   * Sort the partitions in the cluster by the utilization of the given resource.
   * @param resource the resource type.
   * @return a list of partitions sorted by utilization of the given resource.
   */
  public List<Partition> replicasSortedByUtilization(Resource resource) {
    List<Partition> partitionList = new ArrayList<>();
    partitionList.addAll(_partitionsByTopicPartition.values());
    partitionList.sort((o1, o2) -> Double.compare(o2.leader().load().expectedUtilizationFor(resource),
                                                  o1.leader().load().expectedUtilizationFor(resource)));
    return Collections.unmodifiableList(partitionList);
  }

  /**
   * (1) Check whether each load in the cluster contains exactly the number of windows defined by the Load.
   * (2) Check whether sum of loads in the cluster / rack / broker / replica are consistent with each other.
   */
  public void sanityCheck() {
    // SANITY CHECK #1: Each load in the cluster must contain exactly the number of windows defined by the Load.
    Map<String, Integer> errorMsgAndNumWindows = new HashMap<>();

    int expectedNumWindows = _load.numWindows();

    // Check leadership loads.
    for (Map.Entry<Integer, Load> entry : _potentialLeadershipLoadByBrokerId.entrySet()) {
      int brokerId = entry.getKey();
      Load load = entry.getValue();
      if (load.numWindows() != expectedNumWindows && broker(brokerId).replicas().size() != 0) {
        errorMsgAndNumWindows.put("Leadership(" + brokerId + ")", load.numWindows());
      }
    }

    // Check rack loads.
    for (Rack rack : _racksById.values()) {
      if (rack.load().numWindows() != expectedNumWindows && rack.replicas().size() != 0) {
        errorMsgAndNumWindows.put("Rack(id:" + rack.id() + ")", rack.load().numWindows());
      }

      // Check the host load.
      for (Host host : rack.hosts()) {
        if (host.load().numWindows() != expectedNumWindows && host.replicas().size() != 0) {
          errorMsgAndNumWindows.put("Host(id:" + host.name() + ")", host.load().numWindows());
        }

        // Check broker loads.
        for (Broker broker : rack.brokers()) {
          if (broker.load().numWindows() != expectedNumWindows && broker.replicas().size() != 0) {
            errorMsgAndNumWindows.put("Broker(id:" + broker.id() + ")", broker.load().numWindows());
          }

          // Check replica loads.
          for (Replica replica : broker.replicas()) {
            if (replica.load().numWindows() != expectedNumWindows) {
              errorMsgAndNumWindows.put("Replica(id:" + replica.topicPartition() + "-" + broker.id() + ")",
                                         replica.load().numWindows());
            }
          }
        }
      }
    }
    StringBuilder exceptionMsg = new StringBuilder();
    for (Map.Entry<String, Integer> entry : errorMsgAndNumWindows.entrySet()) {
      exceptionMsg.append(String.format("[%s: %d]%n", entry.getKey(), entry.getValue()));
    }

    if (exceptionMsg.length() > 0) {
      throw new IllegalArgumentException("Loads must have all have " + expectedNumWindows + " windows. Following "
                                         + "loads violate this constraint with specified number of windows: " + exceptionMsg);
    }
    // SANITY CHECK #2: Sum of loads in the cluster / rack / broker / replica must be consistent with each other.
    String prologueErrorMsg = "Inconsistent load distribution.";

    // Check equality of sum of the replica load to their broker load for each resource.
    for (Broker broker : brokers()) {
      for (Resource resource : Resource.cachedValues()) {
        double sumOfReplicaUtilization = 0.0;
        for (Replica replica : broker.replicas()) {
          sumOfReplicaUtilization += replica.load().expectedUtilizationFor(resource);
        }
        if (AnalyzerUtils.compare(sumOfReplicaUtilization, broker.load().expectedUtilizationFor(resource), resource) != 0) {
          throw new IllegalArgumentException(prologueErrorMsg + " Broker utilization for " + resource + " is different "
                                             + "from the total replica utilization in the broker with id: " + broker.id()
                                             + ". Sum of the replica utilization: " + sumOfReplicaUtilization
                                             + ", broker utilization: " + broker.load().expectedUtilizationFor(resource));
        }
      }
    }

    // Check equality of sum of the broker load to their rack load for each resource.
    Map<Resource, Double> sumOfRackUtilizationByResource = new HashMap<>();
    for (Rack rack : _racksById.values()) {
      Map<Resource, Double> sumOfHostUtilizationByResource = new HashMap<>();
      for (Host host : rack.hosts()) {
        for (Resource resource : Resource.cachedValues()) {
          sumOfHostUtilizationByResource.putIfAbsent(resource, 0.0);
          double sumOfBrokerUtilization = 0.0;
          for (Broker broker : host.brokers()) {
            sumOfBrokerUtilization += broker.load().expectedUtilizationFor(resource);
          }
          Double hostUtilization = host.load().expectedUtilizationFor(resource);
          if (AnalyzerUtils.compare(sumOfBrokerUtilization, hostUtilization, resource) != 0) {
            throw new IllegalArgumentException(prologueErrorMsg + " Host utilization for " + resource + " is different "
                                               + "from the total broker utilization in the host : " + host.name()
                                               + ". Sum of the brokers: " + sumOfBrokerUtilization
                                               + ", host utilization: " + hostUtilization);
          }
          sumOfHostUtilizationByResource.put(resource, sumOfHostUtilizationByResource.get(resource) + hostUtilization);
        }
      }

      // Check equality of sum of the host load to the rack load for each resource.
      for (Map.Entry<Resource, Double> entry : sumOfHostUtilizationByResource.entrySet()) {
        Resource resource = entry.getKey();
        double sumOfHostsUtil = entry.getValue();
        sumOfRackUtilizationByResource.putIfAbsent(resource, 0.0);
        Double rackUtilization = rack.load().expectedUtilizationFor(resource);
        if (AnalyzerUtils.compare(rackUtilization, sumOfHostsUtil, resource) != 0) {
          throw new IllegalArgumentException(prologueErrorMsg + " Rack utilization for " + resource + " is different "
                                             + "from the total host utilization in rack" + rack.id()
                                             + " . Sum of the hosts: " + sumOfHostsUtil + ", rack utilization: "
                                             + rack.load().expectedUtilizationFor(resource));
        }
        sumOfRackUtilizationByResource.put(resource, sumOfRackUtilizationByResource.get(resource) + sumOfHostUtilizationByResource.get(resource));
      }
    }

    // Check equality of sum of the rack load to the cluster load for each resource.
    for (Map.Entry<Resource, Double> entry : sumOfRackUtilizationByResource.entrySet()) {
      Resource resource = entry.getKey();
      double sumOfRackUtil = entry.getValue();
      if (AnalyzerUtils.compare(_load.expectedUtilizationFor(resource), sumOfRackUtil, resource) != 0) {
        throw new IllegalArgumentException(prologueErrorMsg + " Cluster utilization for " + resource + " is different "
                                           + "from the total rack utilization in the cluster. Sum of the racks: "
                                           + sumOfRackUtil + ", cluster utilization: " + _load.expectedUtilizationFor(resource));
      }
    }

    // Check equality of the sum of the leadership load to the sum of the load of leader at each broker.
    for (Broker broker : brokers()) {
      double sumOfLeaderOfReplicaUtilization = 0.0;
      for (Replica replica : broker.replicas()) {
        sumOfLeaderOfReplicaUtilization +=
            partition(replica.topicPartition()).leader().load().expectedUtilizationFor(Resource.NW_OUT);
      }
      if (AnalyzerUtils.compare(sumOfLeaderOfReplicaUtilization,
                                _potentialLeadershipLoadByBrokerId.get(broker.id()).expectedUtilizationFor(Resource.NW_OUT),
                                Resource.NW_OUT) != 0) {
        throw new IllegalArgumentException(prologueErrorMsg + " Leadership utilization for " + Resource.NW_OUT
                                           + " is different from the total utilization leader of replicas in the broker"
                                           + " with id: " + broker.id() + " Expected: " + sumOfLeaderOfReplicaUtilization
                                           + " Received: " + _potentialLeadershipLoadByBrokerId
                                               .get(broker.id()).expectedUtilizationFor(Resource.NW_OUT) + ".");
      }

      for (Resource resource : Resource.cachedValues()) {
        if (resource == Resource.CPU) {
          continue;
        }
        double leaderSum = broker.leaderReplicas().stream().mapToDouble(r -> r.load().expectedUtilizationFor(resource)).sum();
        double cachedLoad = broker.leadershipLoadForNwResources().expectedUtilizationFor(resource);
        if (AnalyzerUtils.compare(leaderSum, cachedLoad, resource) != 0) {
          throw new IllegalArgumentException(prologueErrorMsg + " Leadership load for resource " + resource + " is "
                                             + cachedLoad + " but recomputed sum is " + leaderSum + ".");
        }
      }
    }
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public List<Map<String, Object>> getJsonStructure() {
    List<Map<String, Object>> finalClusterStats = new ArrayList<>();

    for (Broker broker : brokers()) {
      double leaderBytesInRate = broker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN);

      Map<String, Object> hostEntry = new HashMap<>();
      hostEntry.put("Host", broker.host().name());
      hostEntry.put("Broker", broker.id());
      hostEntry.put("BrokerState", broker.getState());
      hostEntry.put("DiskMB", AnalyzerUtils.nanToZero(broker.load().expectedUtilizationFor(Resource.DISK)));
      hostEntry.put("CpuPct", AnalyzerUtils.nanToZero(broker.load().expectedUtilizationFor(Resource.CPU)));
      hostEntry.put("LeaderNwInRate", AnalyzerUtils.nanToZero(leaderBytesInRate));
      hostEntry.put("FollowerNwInRate", AnalyzerUtils.nanToZero(broker.load().expectedUtilizationFor(Resource.NW_IN) - leaderBytesInRate));
      hostEntry.put("NnwOutRate", AnalyzerUtils.nanToZero(broker.load().expectedUtilizationFor(Resource.NW_OUT)));
      hostEntry.put("PnwOutRate", AnalyzerUtils.nanToZero(potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT)));
      hostEntry.put("Replicas", broker.replicas().size());
      hostEntry.put("LeaderReplicas", broker.leaderReplicas().size());

      finalClusterStats.add(hostEntry);
    }

    return finalClusterStats;
  }

  /**
   * Get broker level stats in JSON format.
   */
  public String brokerStatsJSON() {
    Gson gson = new Gson();
    String json = gson.toJson(getJsonStructure());
    return json;
  }

  /**
   * Get broker return the broker stats.
   */
  public BrokerStats brokerStats() {
    BrokerStats brokerStats = new BrokerStats();
    brokers().forEach(broker -> {
      double leaderBytesInRate = broker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN);
      brokerStats.addSingleBrokerStats(broker.host().name(),
                                       broker.id(),
                                       broker.getState(),
                                       broker.replicas().isEmpty() ? 0 : broker.load().expectedUtilizationFor(Resource.DISK),
                                       broker.load().expectedUtilizationFor(Resource.CPU),
                                       leaderBytesInRate,
                                       broker.load().expectedUtilizationFor(Resource.NW_IN) - leaderBytesInRate,
                                       broker.load().expectedUtilizationFor(Resource.NW_OUT),
                                       potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT),
                                       broker.replicas().size(), broker.leaderReplicas().size(),
                                       _capacityEstimationInfoByBrokerId.get(broker.id()) != null);
    });
    return brokerStats;
  }

  /**
   * Get broker level stats in human readable format.
   */
  public static class BrokerStats {
    private final List<SingleBrokerStats> _brokerStats = new ArrayList<>();
    private int _hostFieldLength = 0;
    private SortedMap<String, BasicStats> _hostStats = new ConcurrentSkipListMap<>();

    private void addSingleBrokerStats(String host, int id, Broker.State state, double diskUtil, double cpuUtil, double leaderBytesInRate,
                                      double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate,
                                      int numReplicas, int numLeaders, boolean isEstimated) {

      SingleBrokerStats singleBrokerStats =
          new SingleBrokerStats(host, id, state, diskUtil, cpuUtil, leaderBytesInRate, followerBytesInRate, bytesOutRate,
                                potentialBytesOutRate, numReplicas, numLeaders, isEstimated);
      _brokerStats.add(singleBrokerStats);
      _hostFieldLength = Math.max(_hostFieldLength, host.length());
      _hostStats.computeIfAbsent(host, h -> new BasicStats(0.0, 0.0, 0.0, 0.0,
                                                           0.0, 0.0, 0, 0))
                .addBasicStats(singleBrokerStats.basicStats());
    }

    /**
     * Get the broker level load stats.
     */
    public List<SingleBrokerStats> stats() {
      return _brokerStats;
    }

    /**
     * Return a valid JSON encoded string
     *
     * @param version JSON version
     */
    public String getJSONString(int version) {
      Gson gson = new Gson();
      Map<String, Object> jsonStructure = getJsonStructure();
      jsonStructure.put("version", version);
      return gson.toJson(jsonStructure);
    }

    /**
     * Return an object that can be further used
     * to encode into JSON
     */
    public Map<String, Object> getJsonStructure() {
      List<Map<String, Object>> hostStats = new ArrayList<>();

      // host level statistics
      for (Map.Entry<String, BasicStats> entry : _hostStats.entrySet()) {
        BasicStats stats = entry.getValue();
        Map<String, Object> hostEntry = entry.getValue().getJSONStructure();
        hostEntry.put("Host", entry.getKey());
        hostStats.add(hostEntry);
      }

      // broker level statistics
      List<Map<String, Object>> brokerStats = new ArrayList<>();
      for (SingleBrokerStats stats : _brokerStats) {
        Map<String, Object> brokerEntry = stats.getJSONStructure();
        brokerStats.add(brokerEntry);
      }

      // consolidated
      Map<String, Object> stats = new HashMap<>();
      stats.put("hosts", hostStats);
      stats.put("brokers", brokerStats);
      return stats;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      // put host stats.
      sb.append(String.format("%n%" + _hostFieldLength + "s%20s%15s%25s%25s%20s%20s%20s%n",
                              "HOST", "DISK(MB)", "CPU(%)", "LEADER_NW_IN(KB/s)",
                              "FOLLOWER_NW_IN(KB/s)", "NW_OUT(KB/s)", "PNW_OUT(KB/s)", "LEADERS/REPLICAS"));
      for (Map.Entry<String, BasicStats> entry : _hostStats.entrySet()) {
        BasicStats stats = entry.getValue();
        sb.append(String.format("%" + _hostFieldLength + "s,%19.3f,%14.3f,%24.3f,%24.3f,%19.3f,%19.3f,%14d/%d%n",
                                entry.getKey(),
                                stats.diskUtil(),
                                stats.cpuUtil(),
                                stats.leaderBytesInRate(),
                                stats.followerBytesInRate(),
                                stats.bytesOutRate(),
                                stats.potentialBytesOutRate(),
                                stats.numLeaders(),
                                stats.numReplicas()));
      }

      // put broker stats.
      sb.append(String.format("%n%n%" + _hostFieldLength + "s%15s%20s%15s%25s%25s%20s%20s%20s%n",
                              "HOST", "BROKER", "DISK(MB)", "CPU(%)", "LEADER_NW_IN(KB/s)",
                              "FOLLOWER_NW_IN(KB/s)", "NW_OUT(KB/s)", "PNW_OUT(KB/s)", "LEADERS/REPLICAS"));
      for (SingleBrokerStats stats : _brokerStats) {
        sb.append(String.format("%" + _hostFieldLength + "s,%14d,%19.3f,%14.3f,%24.3f,%24.3f,%19.3f,%19.3f,%14d/%d%n",
                                stats.host(),
                                stats.id(),
                                stats.basicStats().diskUtil(),
                                stats.basicStats().cpuUtil(),
                                stats.basicStats().leaderBytesInRate(),
                                stats.basicStats().followerBytesInRate(),
                                stats.basicStats().bytesOutRate(),
                                stats.basicStats().potentialBytesOutRate(),
                                stats.basicStats().numLeaders(),
                                stats.basicStats().numReplicas()));
      }

      return sb.toString();
    }
  }

  public static class SingleBrokerStats {
    private final String _host;
    private final int _id;
    private final Broker.State _state;
    final BasicStats _basicStats;
    private final boolean _isEstimated;

    private SingleBrokerStats(String host, int id, Broker.State state, double diskUtil, double cpuUtil, double leaderBytesInRate,
                              double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate,
                              int numReplicas, int numLeaders, boolean isEstimated) {
      _host = host;
      _id = id;
      _state = state;
      _basicStats = new BasicStats(diskUtil, cpuUtil, leaderBytesInRate, followerBytesInRate, bytesOutRate,
                                   potentialBytesOutRate, numReplicas, numLeaders);
      _isEstimated = isEstimated;
    }

    public String host() {
      return _host;
    }

    public Broker.State state() {
      return _state;
    }

    public int id() {
      return _id;
    }

    private BasicStats basicStats() {
      return _basicStats;
    }

    public boolean isEstimated() {
      return _isEstimated;
    }

    /*
    * Return an object that can be further used
    * to encode into JSON
    */
    public Map<String, Object> getJSONStructure() {
      Map<String, Object> entry = _basicStats.getJSONStructure();
      entry.put("Host", _host);
      entry.put("Broker", _id);
      entry.put("BrokerState", _state);
      return entry;
    }
  }

  private static class BasicStats {
    private double _diskUtil;
    private double _cpuUtil;
    private double _leaderBytesInRate;
    private double _followerBytesInRate;
    private double _bytesOutRate;
    private double _potentialBytesOutRate;
    private int _numReplicas;
    private int _numLeaders;

    private BasicStats(double diskUtil, double cpuUtil, double leaderBytesInRate,
                       double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate,
                       int numReplicas, int numLeaders) {
      _diskUtil = diskUtil < 0.0 ? 0.0 : diskUtil;
      _cpuUtil = cpuUtil < 0.0 ? 0.0 : cpuUtil;
      _leaderBytesInRate = leaderBytesInRate < 0.0 ? 0.0 : leaderBytesInRate;
      _followerBytesInRate = followerBytesInRate < 0.0 ? 0.0 : followerBytesInRate;
      _bytesOutRate = bytesOutRate < 0.0 ? 0.0 : bytesOutRate;
      _potentialBytesOutRate =  potentialBytesOutRate < 0.0 ? 0.0 : potentialBytesOutRate;
      _numReplicas = numReplicas < 1 ? 0 : numReplicas;
      _numLeaders =  numLeaders < 1 ? 0 : numLeaders;
    }

    double diskUtil() {
      return _diskUtil;
    }

    double cpuUtil() {
      return _cpuUtil;
    }

    double leaderBytesInRate() {
      return _leaderBytesInRate;
    }

    double followerBytesInRate() {
      return _followerBytesInRate;
    }

    double bytesOutRate() {
      return _bytesOutRate;
    }

    double potentialBytesOutRate() {
      return _potentialBytesOutRate;
    }

    int numReplicas() {
      return _numReplicas;
    }

    int numLeaders() {
      return _numLeaders;
    }

    void addBasicStats(BasicStats basicStats) {
      _diskUtil += basicStats.diskUtil();
      _cpuUtil += basicStats.cpuUtil();
      _leaderBytesInRate += basicStats.leaderBytesInRate();
      _followerBytesInRate += basicStats.followerBytesInRate();
      _bytesOutRate += basicStats.bytesOutRate();
      _potentialBytesOutRate  += basicStats.potentialBytesOutRate();
      _numReplicas += basicStats.numReplicas();
      _numLeaders += basicStats.numLeaders();
    }

    /*
    * Return an object that can be further used
    * to encode into JSON
    */
    public Map<String, Object> getJSONStructure() {
      Map<String, Object> entry = new HashMap<>();
      entry.put("DiskMB", diskUtil());
      entry.put("CpuPct", cpuUtil());
      entry.put("LeaderNwInRate", leaderBytesInRate());
      entry.put("FollowerNwInRate", followerBytesInRate());
      entry.put("NnwOutRate", bytesOutRate());
      entry.put("PnwOutRate", potentialBytesOutRate());
      entry.put("Replicas", numReplicas());
      entry.put("Leaders", numLeaders());
      return entry;
    }
  }

  /**
   * The variance of the derived resources.
   * @return a non-null array where the ith index is the variance of RawAndDerivedResource.ordinal().
   */
  public double[] variance() {
    RawAndDerivedResource[] resources = RawAndDerivedResource.values();
    double[][] utilization = utilizationMatrix();

    double[] variance = new double[resources.length];

    for (int resourceIndex = 0; resourceIndex < resources.length; resourceIndex++) {
      Variance varianceCalculator = new Variance();
      variance[resourceIndex] = varianceCalculator.evaluate(utilization[resourceIndex]);
    }
    return variance;
  }

  /**
   *
   * @return a RawAndDerivedResource x nBroker matrix of derived resource utilization.
   */
  public double[][] utilizationMatrix() {
    RawAndDerivedResource[] resources = RawAndDerivedResource.values();
    double[][] utilization = new double[resources.length][brokers().size()];
    int brokerIndex = 0;
    for (Broker broker : brokers()) {
      double leaderBytesInRate = broker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN);
      for (RawAndDerivedResource derivedResource : resources) {
        switch (derivedResource) {
          case DISK: //fall through
          case NW_OUT: //fall through
          case CPU:  utilization[derivedResource.ordinal()][brokerIndex] =
              broker.load().expectedUtilizationFor(derivedResource.derivedFrom());
            break;
          case FOLLOWER_NW_IN:
            utilization[derivedResource.ordinal()][brokerIndex] =
              broker.load().expectedUtilizationFor(derivedResource.derivedFrom()) - leaderBytesInRate;
            break;
          case LEADER_NW_IN:
            utilization[derivedResource.ordinal()][brokerIndex] = leaderBytesInRate;
            break;
          case PWN_NW_OUT:
            utilization[derivedResource.ordinal()][brokerIndex] =
              potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT);
            break;
          case REPLICAS:
            utilization[derivedResource.ordinal()][brokerIndex] = broker.replicas().size();
            break;
          default:
            throw new IllegalStateException("Unhandled case " + derivedResource + ".");
        }
      }
      brokerIndex++;
    }
    return utilization;
  }

  /**
   * Return a valid JSON encoded string
   *
   * @param version JSON version
   */
  public String getJSONString(int version) {
    Gson gson = new Gson();
    Map<String, Object> jsonStructure = getJsonStructure2();
    jsonStructure.put("version", version);
    return gson.toJson(jsonStructure);
  }

  /**
   * Return an object that can be further used
   * to encode into JSON (version2 thats used in writeTo)
   */
  public Map<String, Object> getJsonStructure2() {
    Map<String, Object> clusterMap = new HashMap<>();
    clusterMap.put("maxPartitionReplicationFactor", _maxReplicationFactor);
    List<Map<String, Object>> racks = new ArrayList<>();
    for (Rack rack : _racksById.values()) {
      racks.add(rack.getJsonStructure());
    }
    clusterMap.put("racks", racks);
    return clusterMap;
  }

  public void writeTo(OutputStream out) throws IOException {
    String cluster = String.format("<Cluster maxPartitionReplicationFactor=\"%d\">%n", _maxReplicationFactor);
    out.write(cluster.getBytes(StandardCharsets.UTF_8));
    for (Rack rack : _racksById.values()) {
      rack.writeTo(out);
    }
    out.write("</Cluster>".getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public String toString() {
    StringBuilder bldr = new StringBuilder();
    bldr.append("ClusterModel[brokerCount=").append(this.brokers().size())
        .append(",partitionCount=").append(_partitionsByTopicPartition.size())
        .append(",healthyBrokerCount=").append(_healthyBrokers.size())
        .append(']');
    return bldr.toString();
  }

  private void refreshCapacity() {
    for (Resource r : Resource.cachedValues()) {
      double capacity = 0;
      for (Rack rack : _racksById.values()) {
        capacity += rack.capacityFor(r);
      }
      _clusterCapacity[r.id()] = capacity;
    }
  }
}
