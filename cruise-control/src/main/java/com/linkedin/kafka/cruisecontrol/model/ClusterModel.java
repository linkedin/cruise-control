/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;

import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.Snapshot;
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
import java.util.concurrent.ConcurrentHashMap;
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
  private final Set<Broker> _newBrokers;
  private final Set<Broker> _healthyBrokers;
  private final double _monitoredPartitionsPercentage;
  private final double[] _clusterCapacity;
  private Load _load;
  // An integer to keep track of the maximum replication factor that a partition was ever created with.
  private int _maxReplicationFactor;
  private Map<Integer, Load> _potentialLeadershipLoadByBrokerId;
  private Set<Long> _validSnapshotTimes;

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
    // A set of newly added brokers
    _newBrokers = new HashSet<>();
    // A set of healthy brokers
    _healthyBrokers = new HashSet<>();
    // Initially cluster does not contain any load.
    _load = Load.newLoad();
    _clusterCapacity = new double[Resource.cachedValues().size()];
    _maxReplicationFactor = 1;
    _potentialLeadershipLoadByBrokerId = new HashMap<>();
    _validSnapshotTimes = new HashSet<>();
    _monitoredPartitionsPercentage = monitoredPartitionsPercentage;
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
  public ClusterModelStats getClusterStats(BalancingConstraint balancingConstraint) throws ModelInputException {
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
   * map: topic-partition -> broker-id-of-replicas. broker-id-of-replicas[0] represents the leader's broker id.
   *
   * @return The replica distribution of leader and follower replicas in the cluster at the point of call.
   */
  public Map<TopicPartition, List<Integer>> getReplicaDistribution()
      throws ModelInputException {
    Map<TopicPartition, List<Integer>> replicaDistribution = new HashMap<>();

    for (Map.Entry<TopicPartition, Partition> entry : _partitionsByTopicPartition.entrySet()) {
      TopicPartition tp = entry.getKey();
      Partition partition = entry.getValue();
      List<Integer> brokerIds = new ArrayList<>();
      // Set broker id of the leader.
      brokerIds.add(partition.leader().broker().id());
      // Set broker id of followers.
      brokerIds.addAll(
          partition.followers().stream().map(follower -> follower.broker().id()).collect(Collectors.toList()));
      // Add distribution of replicas in the partition.
      replicaDistribution.put(tp, brokerIds);
    }

    return replicaDistribution;
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
   * Get partition of the given replica.
   *
   * @param topicPartition Topic partition of the replica for which the partition is requested.
   * @return Partition of the given replica.
   */
  public Partition partition(TopicPartition topicPartition) {
    return _partitionsByTopicPartition.get(topicPartition);
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
        break;
      case NEW:
        _newBrokers.add(broker);
        // fall through to remove the replicas from selfHealingEligibleReplicas
      case ALIVE:
        _selfHealingEligibleReplicas.removeAll(broker.replicas());
        _healthyBrokers.add(broker);
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
   * @param topicPartition      Partition Info of the replica to be relocated.
   * @param sourceBrokerId      Source broker id.
   * @param destinationBrokerId Destination broker id.
   * @throws AnalysisInputException
   */
  public void relocateReplica(TopicPartition topicPartition, int sourceBrokerId, int destinationBrokerId)
      throws AnalysisInputException {
    // Removes the replica and related load from the source broker / source rack / cluster.
    Replica replica = removeReplica(sourceBrokerId, topicPartition);
    if (replica == null) {
      throw new AnalysisInputException("Replica is not in the cluster.");
    }
    // Updates the broker of the removed replica with destination broker.
    replica.setBroker(broker(destinationBrokerId));

    // Add this replica and related load to the destination broker / destination rack / cluster.
    replica.broker().rack().addReplica(replica);
    _load.addLoad(replica.load());
    // Add leadership load to the destination replica.
    _potentialLeadershipLoadByBrokerId.get(destinationBrokerId).addLoad(partition(topicPartition).leader().load());
  }

  /**
   * (1) Removes leadership from source replica.
   * (2) Adds this leadership to the destination replica.
   * (3) Transfers the outbound network load of source replica to the destination replica.
   * (4) Updates the leader and list of followers of the partition.
   *
   * @param topicPartition      Topic partition of this replica.
   * @param sourceBrokerId      Source broker id.
   * @param destinationBrokerId Destination broker id.
   * @return True if relocation is successful, false otherwise.
   * @throws ModelInputException
   */
  public boolean relocateLeadership(TopicPartition topicPartition, int sourceBrokerId, int destinationBrokerId)
      throws ModelInputException {
    // Sanity check to see if the source replica is the leader.
    Replica sourceReplica = _partitionsByTopicPartition.get(topicPartition).replica(sourceBrokerId);
    if (!sourceReplica.isLeader()) {
      return false;
    }
    // Sanity check to see if the destination replica is a follower.
    Replica destinationReplica = _partitionsByTopicPartition.get(topicPartition).replica(destinationBrokerId);
    if (destinationReplica.isLeader()) {
      throw new ModelInputException("Cannot relocate leadership of partition " + topicPartition + "from broker " +
          sourceBrokerId + " to broker " + destinationBrokerId + " because the destination replica is a leader.");
    }

    /**
     * Transfer the outbound network load of source replica to the destination replica.
     * (1) Remove and get the outbound network load associated with leadership from the given replica.
     * (2) Add the outbound network load associated with leadership to the given replica.
     */

    // Remove the load from the source rack.
    Rack rack = broker(sourceBrokerId).rack();
    Map<Resource, Map<Long, Double>> leadershipLoadBySnapshotTime = rack.makeFollower(sourceBrokerId, topicPartition);
    // Add the load to the destination rack.
    rack = broker(destinationBrokerId).rack();
    rack.makeLeader(destinationBrokerId, topicPartition, leadershipLoadBySnapshotTime);

    // Update the leader and list of followers of the partition.
    Partition partition = _partitionsByTopicPartition.get(topicPartition);
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
  public Set<Broker> deadBrokers() {
    Set<Broker> brokers = brokers();
    brokers.removeAll(healthyBrokers());
    return brokers;
  }

  /**
   * Get the set of new brokers.
   */
  public Set<Broker> newBrokers() {
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
    _validSnapshotTimes.clear();
    _load.clearLoad();
  }

  /**
   * Remove and get removed replica from the cluster.
   *
   * @param brokerId       Id of the broker containing the partition.
   * @param topicPartition Topic partition of the replica to be removed.
   * @return The requested replica if the id exists in the rack and the partition is found in the broker, null
   * otherwise.
   */
  public Replica removeReplica(int brokerId, TopicPartition topicPartition) {
    for (Rack rack : _racksById.values()) {
      // Remove the replica and the associated load from the rack that it resides in.
      Replica removedReplica = rack.removeReplica(brokerId, topicPartition);
      if (removedReplica != null) {
        // Remove the load of the removed replica from the recent load of the cluster.
        _load.subtractLoad(removedReplica.load());
        _potentialLeadershipLoadByBrokerId.get(brokerId).subtractLoad(partition(topicPartition).leader().load());
        // Return the removed replica.
        return removedReplica;
      }
    }
    return null;
  }

  /**
   * Get the set of brokers in the cluster.
   */
  public Set<Broker> brokers() {
    Set<Broker> brokers = new HashSet<>();
    for (Rack rack : _racksById.values()) {
      brokers.addAll(rack.brokers());
    }
    return brokers;
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
   * Clear the content and structure of the cluster.
   */
  public void clear() {
    _racksById.clear();
    _partitionsByTopicPartition.clear();
    _load.clearLoad();
    _maxReplicationFactor = 1;
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
   * Push the latest snapshot information containing the snapshot time and resource loads to the cluster.
   *
   * @param rackId         Rack id.
   * @param brokerId       Broker Id containing the replica with the given topic partition.
   * @param topicPartition Topic partition that identifies the replica in this broker.
   * @param snapshot       Snapshot containing latest state for each resource.
   */
  public void pushLatestSnapshot(String rackId, int brokerId, TopicPartition topicPartition, Snapshot snapshot)
      throws ModelInputException {
    // Sanity check for the attempts to push more than allowed number of snapshots having different times.
    if (_validSnapshotTimes.add(snapshot.time()) && _validSnapshotTimes.size() > Load.maxNumSnapshots()) {
      throw new ModelInputException("Cluster cannot contain snapshots for more than " + Load.maxNumSnapshots()
          + " unique snapshot times.");
    }
    Rack rack = rack(rackId);
    rack.pushLatestSnapshot(brokerId, topicPartition, snapshot);

    // Update the recent load of cluster.
    _load.addSnapshot(snapshot);
    // If this snapshot belongs to leader, update leadership load.
    Replica leader = partition(topicPartition).leader();
    if (leader != null && leader.broker().id() == brokerId) {
      // Leadership load must be updated for each broker containing a replica of the same partition.
      for (Replica follower : partition(topicPartition).followers()) {
        _potentialLeadershipLoadByBrokerId.get(follower.broker().id()).addSnapshot(snapshot);
      }
      // Make this update for the broker containing the leader as well.
      _potentialLeadershipLoadByBrokerId.get(brokerId).addSnapshot(snapshot);
    }
  }

  /**
   * Create a replica under given cluster/rack/broker. Add replica to rack and corresponding partition. Get the
   * created replica. If the rack or broker does not exist, create them with UNKNOWN host name. This allows handling
   * of cases where the information of a dead broker is no longer available.
   *
   * @param rackId         Rack id under which the replica will be created.
   * @param brokerId       Broker id under which the replica will be created.
   * @param topicPartition Topic partition information of the replica.
   * @param isLeader         True if the replica is a leader, false otherwise.
   * @param brokerCapacity The broker capacity to use if the broker does not exist.
   * @return Created replica.
   * @throws ModelInputException
   */
  public Replica createReplicaHandleDeadBroker(String rackId, int brokerId, TopicPartition topicPartition, boolean isLeader,
                                               Map<Resource, Double> brokerCapacity) throws ModelInputException {
    if (rack(rackId) == null) {
      createRack(rackId);
    }
    if (broker(brokerId) == null) {
      createBroker(rackId, "UNKNOWN_HOST", brokerId, brokerCapacity);
    }
    return createReplica(rackId, brokerId, topicPartition, isLeader);
  }

  /**
   * Create a replica under given cluster/rack/broker. Add replica to rack and corresponding partition. Get the
   * created replica.
   *
   * @param rackId         Rack id under which the replica will be created.
   * @param brokerId       Broker id under which the replica will be created.
   * @param topicPartition Topic partition information of the replica.
   * @param isLeader         True if the replica is a leader, false otherwise.
   * @return
   */
  public Replica createReplica(String rackId, int brokerId, TopicPartition topicPartition, boolean isLeader)
      throws ModelInputException {
    Replica replica = new Replica(topicPartition, broker(brokerId), isLeader);
    rack(rackId).addReplica(replica);

    // Add replica to its partition.
    if (!_partitionsByTopicPartition.containsKey(topicPartition)) {
      // Partition has not been created before.
      _partitionsByTopicPartition.put(topicPartition, new Partition(topicPartition, null));
    }

    Partition partition = _partitionsByTopicPartition.get(topicPartition);
    if (replica.isLeader()) {
      partition.setLeader(replica);
      return replica;
    }

    partition.addFollower(replica);
    // If leader of this follower was already created and load was pushed to it, add that load to the follower.
    Replica leaderReplica = partition(topicPartition).leader();
    if (leaderReplica != null) {
      _potentialLeadershipLoadByBrokerId.get(brokerId).addLoad(leaderReplica.load());
    }

    // Increment the maximum replication factor if the number of replicas of the partition is larger than the
    //  maximum replication factor of previously existing partitions.
    _maxReplicationFactor = Math.max(_maxReplicationFactor, partition.followers().size() + 1);

    return replica;
  }

  /**
   * Create a broker under this cluster/rack and get the created broker.
   *
   * @param rackId         Id of the rack that the broker will be created in.
   * @param host           The host of this broker
   * @param brokerId       Id of the broker to be created.
   * @param brokerCapacity Capacity of the created broker.
   * @return Created broker.
   */
  public Broker createBroker(String rackId, String host, int brokerId, Map<Resource, Double> brokerCapacity) {
    _potentialLeadershipLoadByBrokerId.putIfAbsent(brokerId, Load.newLoad());
    Rack rack = rack(rackId);
    _brokerIdToRack.put(brokerId, rack);
    Broker broker = rack.createBroker(brokerId, host, brokerCapacity);
    _healthyBrokers.add(broker);
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
   * (given utilization threshold) * (broker capacity). Utilization threshold might any capacity constraint
   * thresholds such as balance or capacity.
   *
   * @param resource             Resource for which brokers will be sorted.
   * @param utilizationThreshold Utilization threshold for the given resource.
   * @return A list of sorted (in ascending order by resource) healthy brokers having utilization under:
   * (given utilization threshold) * (broker capacity).
   */
  public List<Broker> sortedHealthyBrokersUnderThreshold(Resource resource, double utilizationThreshold) {
    List<Broker> sortedTargetBrokersUnderCapacityLimit = new ArrayList<>();

    for (Broker healthyBroker : healthyBrokers()) {
      double brokerCapacityLimit = healthyBroker.capacityFor(resource) * utilizationThreshold;
      double brokerUtilization = healthyBroker.load().expectedUtilizationFor(resource);
      double hostCapacityLimit = healthyBroker.host().capacityFor(resource) * utilizationThreshold;
      double hostUtilization = healthyBroker.host().load().expectedUtilizationFor(resource);
      if (brokerUtilization < brokerCapacityLimit
          && (!resource.isHostResource() || hostUtilization < hostCapacityLimit)) {
        sortedTargetBrokersUnderCapacityLimit.add(healthyBroker);
      }
    }
    sortedTargetBrokersUnderCapacityLimit.sort((o1, o2) -> {
      Double expectedBrokerLoad1 = o1.load().expectedUtilizationFor(resource);
      Double expectedBrokerLoad2 = o2.load().expectedUtilizationFor(resource);
      // For host resource we first compare host util then look at the broker util.
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

  /**
   * Sort replicas in ascending order of resource quantity present in the broker that they reside in terms of the
   * requested resource.
   *
   * @param replicas A list of replicas to be sorted by the amount of resources that their broker contains.
   * @param resource Resource for which the given replicas will be sorted.
   */
  public void sortReplicasInAscendingOrderByBrokerResourceUtilization(List<Replica> replicas, Resource resource) {
    replicas.sort((o1, o2) -> {
      Double expectedBrokerLoad1 = o1.broker().load().expectedUtilizationFor(resource);
      Double expectedBrokerLoad2 = o2.broker().load().expectedUtilizationFor(resource);
      return Double.compare(expectedBrokerLoad1, expectedBrokerLoad2);
    });
  }

  /**
   * Sort brokers in the cluster in ascending order by their broker id.
   *
   * @return Sorted list of brokers.
   */
  public List<Broker> sortBrokersInAscendingOrderById() {
    List<Broker> brokers = new ArrayList<>(brokers());
    Collections.sort(brokers, (o1, o2) -> Integer.compare(o1.id(), o2.id()));
    return brokers;
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
   * (1) Check whether each load in the cluster contains exactly the number of snapshots defined by the Load.
   * (2) Check whether sum of loads in the cluster / rack / broker / replica are consistent with each other.
   */
  public void sanityCheck() throws ModelInputException {
    // SANITY CHECK #1: Each load in the cluster must contain exactly the number of snapshots defined by the Load.
    Map<String, Integer> numSnapshotsByErrorMsg = new HashMap<>();

    int expectedNumSnapshots = _load.numSnapshots();

    // Check leadership loads.
    for (Map.Entry<Integer, Load> entry : _potentialLeadershipLoadByBrokerId.entrySet()) {
      int brokerId = entry.getKey();
      Load load = entry.getValue();
      if (load.numSnapshots() != expectedNumSnapshots && broker(brokerId).replicas().size() != 0) {
        numSnapshotsByErrorMsg.put("Leadership(" + brokerId + ")", load.numSnapshots());
      }
    }

    // Check rack loads.
    for (Rack rack : _racksById.values()) {
      if (rack.load().numSnapshots() != expectedNumSnapshots && rack.replicas().size() != 0) {
        numSnapshotsByErrorMsg.put("Rack(id:" + rack.id() + ")", rack.load().numSnapshots());
      }

      // Check the host load.
      for (Host host : rack.hosts()) {
        if (host.load().numSnapshots() != expectedNumSnapshots && host.replicas().size() != 0) {
          numSnapshotsByErrorMsg.put("Host(id:" + host.name() + ")", host.load().numSnapshots());
        }

        // Check broker loads.
        for (Broker broker : rack.brokers()) {
          if (broker.load().numSnapshots() != expectedNumSnapshots && broker.replicas().size() != 0) {
            numSnapshotsByErrorMsg.put("Broker(id:" + broker.id() + ")", broker.load().numSnapshots());
          }

          // Check replica loads.
          for (Replica replica : broker.replicas()) {
            if (replica.load().numSnapshots() != expectedNumSnapshots) {
              numSnapshotsByErrorMsg.put("Replica(id:" + replica.topicPartition() + "-" + broker.id() + ")",
                                         replica.load().numSnapshots());
            }
          }
        }
      }
    }
    StringBuilder exceptionMsg = new StringBuilder();
    for (Map.Entry<String, Integer> entry : numSnapshotsByErrorMsg.entrySet()) {
      exceptionMsg.append(String.format("[%s: %d]%n", entry.getKey(), entry.getValue()));
    }

    if (exceptionMsg.length() > 0) {
      throw new ModelInputException("Loads must have all have " + expectedNumSnapshots + " snapshots. "
          + "Following loads violate this constraint with specified number of snapshots: " + exceptionMsg);
    }
    // SANITY CHECK #2: Sum of loads in the cluster / rack / broker / replica must be consistent with each other.
    String prologueErrorMsg = "Inconsistent load distribution.";

    // Check equality of sum of the replica load to their broker load for each resource.
    for (Broker broker : brokers()) {
      for (Resource resource : Resource.values()) {
        double sumOfReplicaUtilization = 0.0;
        for (Replica replica : broker.replicas()) {
          sumOfReplicaUtilization += replica.load().expectedUtilizationFor(resource);
        }
        if (AnalyzerUtils.compare(sumOfReplicaUtilization, broker.load().expectedUtilizationFor(resource), resource) != 0) {
          throw new ModelInputException(prologueErrorMsg + " Broker utilization for " + resource + " is "
              + "different from the total replica utilization in the broker with id: " + broker.id()
              + ". Sum of the replica utilization: " + sumOfReplicaUtilization + ", broker utilization: "
              + broker.load().expectedUtilizationFor(resource));
        }
      }
    }

    // Check equality of sum of the broker load to their rack load for each resource.
    Map<Resource, Double> sumOfRackUtilizationByResource = new HashMap<>();
    for (Rack rack : _racksById.values()) {
      Map<Resource, Double> sumOfHostUtilizationByResource = new HashMap<>();
      for (Host host : rack.hosts()) {
        for (Resource resource : Resource.values()) {
          sumOfHostUtilizationByResource.putIfAbsent(resource, 0.0);
          double sumOfBrokerUtilization = 0.0;
          for (Broker broker : host.brokers()) {
            sumOfBrokerUtilization += broker.load().expectedUtilizationFor(resource);
          }
          Double hostUtilization = host.load().expectedUtilizationFor(resource);
          if (AnalyzerUtils.compare(sumOfBrokerUtilization, hostUtilization, resource) != 0) {
            throw new ModelInputException(prologueErrorMsg + " Host utilization for " + resource + " is "
                                              + "different from the total broker utilization in the host : " + host.name()
                                              + ". Sum of the brokers: " + sumOfBrokerUtilization + ", host utilization: " + hostUtilization);
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
          throw new ModelInputException(prologueErrorMsg + " Rack utilization for " + resource + " is "
                                            + "different from the total host utilization in rack" + rack.id()
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
        throw new ModelInputException(prologueErrorMsg + " Cluster utilization for " + resource + " is "
            + "different from the total rack utilization in the cluster. Sum of the racks: "
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
        throw new ModelInputException(prologueErrorMsg + " Leadership utilization for " + Resource.NW_OUT
            + " is different from the total utilization leader of replicas in the broker with id: "
            + broker.id() + " Expected: " + sumOfLeaderOfReplicaUtilization + " Received: "
            + _potentialLeadershipLoadByBrokerId.get(broker.id()).expectedUtilizationFor(Resource.NW_OUT) + ".");
      }

      for (Resource resource : Resource.values()) {
        if (resource == Resource.CPU) {
          continue;
        }
        double leaderSum = broker.leaderReplicas().stream().mapToDouble(r -> r.load().expectedUtilizationFor(resource)).sum();
        double cachedLoad = broker.leadershipLoad().expectedUtilizationFor(resource);
        if (AnalyzerUtils.compare(leaderSum, cachedLoad, resource) != 0) {
          throw new ModelInputException(prologueErrorMsg + " Leadership load for resource " + resource + " is " +
            cachedLoad + " but recomputed sum is " + leaderSum + ".");
        }
      }
    }
  }

  /**
   * Get broker return the broker stats.
   */
  public BrokerStats brokerStats() {
    BrokerStats brokerStats = new BrokerStats();
    sortBrokersInAscendingOrderById()
        .forEach(broker -> {
          double leaderBytesInRate = broker.leadershipLoad().expectedUtilizationFor(Resource.NW_IN);
          brokerStats.addSingleBrokerStats(broker.host().name(),
                                           broker.id(),
                                           broker.load().expectedUtilizationFor(Resource.DISK),
                                           broker.load().expectedUtilizationFor(Resource.CPU),
                                           leaderBytesInRate,
                                           broker.load().expectedUtilizationFor(Resource.NW_IN) - leaderBytesInRate,
                                           broker.load().expectedUtilizationFor(Resource.NW_OUT),
                                           potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT),
                                           broker.replicas().size());
        });
    return brokerStats;
  }

  /**
   * Get broker level stats in human readable format.
   */
  public static class BrokerStats {
    private final List<SingleBrokerStats> _brokerStats = new ArrayList<>();
    private int _hostFieldLength = 0;
    private Map<String, BasicStats> _hostStats = new ConcurrentHashMap<>();

    private void addSingleBrokerStats(String host, int id, double diskUtil, double cpuUtil, double leaderBytesInRate,
                                      double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate,
                                      int numReplicas) {

      SingleBrokerStats singleBrokerStats =
          new SingleBrokerStats(host, id, diskUtil, cpuUtil, leaderBytesInRate, followerBytesInRate, bytesOutRate,
                                potentialBytesOutRate, numReplicas);
      _brokerStats.add(singleBrokerStats);
      _hostFieldLength = Math.max(_hostFieldLength, host.length());
      _hostStats.computeIfAbsent(host, h -> new BasicStats(0.0, 0.0, 0.0, 0.0,
                                                           0.0, 0.0, 0))
                .addBasicStats(singleBrokerStats.basicStats());
    }

    /**
     * Get the broker level load stats.
     */
    public List<SingleBrokerStats> stats() {
      return _brokerStats;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      // put host stats.
      sb.append(String.format("%n%" + _hostFieldLength + "s%20s%15s%25s%25s%20s%20s%15s%n",
                              "HOST", "DISK(MB)", "CPU(%)", "LEADER_NW_IN(KB/s)",
                              "FOLLOWER_NW_IN(KB/s)", "NW_OUT(KB/s)", "PNW_OUT(KB/s)", "REPLICAS"));
      for (Map.Entry<String, BasicStats> entry : _hostStats.entrySet()) {
        BasicStats stats = entry.getValue();
        sb.append(String.format("%" + _hostFieldLength + "s,%19.3f,%14.3f,%24.3f,%24.3f,%19.3f,%19.3f,%14d%n",
                                entry.getKey(),
                                stats.diskUtil(),
                                stats.cpuUtil(),
                                stats.leaderBytesInRate(),
                                stats.followerBytesInRate(),
                                stats.bytesOutRate(),
                                stats.potentialBytesOutRate(),
                                stats.numReplicas()));
      }

      // put broker stats.
      sb.append(String.format("%n%n%" + _hostFieldLength + "s%15s%20s%15s%25s%25s%20s%20s%15s%n",
                              "HOST", "BROKER", "DISK(MB)", "CPU(%)", "LEADER_NW_IN(KB/s)",
                              "FOLLOWER_NW_IN(KB/s)", "NW_OUT(KB/s)", "PNW_OUT(KB/s)", "REPLICAS"));
      for (SingleBrokerStats stats : _brokerStats) {
        sb.append(String.format("%" + _hostFieldLength + "s,%14d,%19.3f,%14.3f,%24.3f,%24.3f,%19.3f,%19.3f,%14d%n",
                                stats.host(),
                                stats.id(),
                                stats.basicStats().diskUtil(),
                                stats.basicStats().cpuUtil(),
                                stats.basicStats().leaderBytesInRate(),
                                stats.basicStats().followerBytesInRate(),
                                stats.basicStats().bytesOutRate(),
                                stats.basicStats().potentialBytesOutRate(),
                                stats.basicStats().numReplicas()));
      }

      return sb.toString();
    }
  }

  public static class SingleBrokerStats {
    private final String _host;
    private final int _id;
    final BasicStats _basicStats;

    private SingleBrokerStats(String host, int id, double diskUtil, double cpuUtil, double leaderBytesInRate,
                              double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate,
                              int numReplicas) {
      _host = host;
      _id = id;
      _basicStats = new BasicStats(diskUtil, cpuUtil, leaderBytesInRate, followerBytesInRate, bytesOutRate,
                                   potentialBytesOutRate, numReplicas);
    }

    public String host() {
      return _host;
    }

    public int id() {
      return _id;
    }

    private BasicStats basicStats() {
      return _basicStats;
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

    private BasicStats(double diskUtil, double cpuUtil, double leaderBytesInRate,
                       double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate,
                       int numReplicas) {
      _diskUtil = diskUtil;
      _cpuUtil = cpuUtil;
      _leaderBytesInRate = leaderBytesInRate;
      _followerBytesInRate = followerBytesInRate;
      _bytesOutRate = bytesOutRate;
      _potentialBytesOutRate = potentialBytesOutRate;
      _numReplicas = numReplicas;
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

    void addBasicStats(BasicStats basicStats) {
      _diskUtil += basicStats.diskUtil();
      _cpuUtil += basicStats.cpuUtil();
      _leaderBytesInRate += basicStats.leaderBytesInRate();
      _followerBytesInRate += basicStats.followerBytesInRate();
      _bytesOutRate += basicStats.bytesOutRate();
      _potentialBytesOutRate  += basicStats.potentialBytesOutRate();
      _numReplicas += basicStats.numReplicas();
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
    List<Broker> brokers = sortBrokersInAscendingOrderById();
    for (int brokerIndex = 0; brokerIndex < brokers.size(); brokerIndex++) {
      double leaderBytesInRate = 0.0;
      Broker broker = brokers.get(brokerIndex);
      for (Replica leaderReplica : broker.leaderReplicas()) {
        leaderBytesInRate += leaderReplica.load().expectedUtilizationFor(Resource.NW_IN);
      }
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
    }
    return utilization;
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
    bldr.append("ClusterModel[brokerCount=").append(this.brokers().size()).append(",partitionCount=")
      .append(_partitionsByTopicPartition.size()).append(']');
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
