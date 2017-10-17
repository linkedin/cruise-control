/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;

import com.linkedin.kafka.cruisecontrol.monitor.sampling.Snapshot;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;


/**
 * A class that holds the information of the broker, including its liveness and load for replicas. A broker object is
 * created as part of a rack structure.
 */
public class Broker implements Serializable {

  public enum State {
    ALIVE, DEAD, NEW
  }

  private final int _id;
  private final Host _host;
  private final double[] _brokerCapacity;
  private final Set<Replica> _replicas;
  private final Set<Replica> _leaderReplicas;
  /** Set of immigrant replicas */
  private final Set<Replica> _immigrantReplicas;
  /** A map for tracking topic -> (partitionId -> replica). */
  private final Map<String, Map<Integer, Replica>> _topicReplicas;
  private final Load _load;
  private final Load _leadershipLoad;
  private State _state;

  /**
   * Constructor for Broker class.
   *
   * @param host           The host this broker is on
   * @param id             The id of the broker.
   * @param brokerCapacity The capacity of the broker.
   */
  Broker(Host host, int id, Map<Resource, Double> brokerCapacity) {
    _host = host;
    _id = id;
    _brokerCapacity = new double[Resource.values().length];
    for (Map.Entry<Resource, Double> entry : brokerCapacity.entrySet()) {
      _brokerCapacity[entry.getKey().id()] = entry.getValue();
    }
    _replicas = new HashSet<>();
    _leaderReplicas = new HashSet<>();
    _topicReplicas = new HashMap<>();
    _immigrantReplicas = new HashSet<>();
    // Initially broker does not contain any load.
    _load = Load.newLoad();
    _leadershipLoad = Load.newLoad();
    _state = State.ALIVE;
  }

  public Host host() {
    return _host;
  }

  /**
   * Get broker's rack.
   */
  public Rack rack() {
    return _host.rack();
  }

  /**
   * Get broker Id.
   */
  public int id() {
    return _id;
  }

  /**
   * Get broker capacity for the requested resource.
   *
   * @param resource Resource for which the capacity will be provided.
   * @return If broker is alive, the capacity of the requested resource, -1.0 otherwise.
   */
  public double capacityFor(Resource resource) {
    if (isAlive()) {
      return _brokerCapacity[resource.id()];
    }
    return -1.0;
  }

  /**
   * Get replicas residing in the broker.
   */
  public Set<Replica> replicas() {
    return _replicas;
  }

  /**
   * Get all the leader replicas.
   */
  public Set<Replica> leaderReplicas() {
    return _leaderReplicas;
  }

  /**
   * Get the immigrant replicas (The replicas that are moved here).
   */
  public Set<Replica> immigrantReplicas() {
    return _immigrantReplicas;
  }

  /**
   * Get the replica if it is in the broker.
   *
   * @param topicPartition Topic partition of the replica.
   * @return Replica if it exists in the broker, null otherwise.
   */
  public Replica replica(TopicPartition topicPartition) {
    Map<Integer, Replica> topicReplicas = _topicReplicas.get(topicPartition.topic());
    if (topicReplicas == null) {
      return null;
    }

    return topicReplicas.get(topicPartition.partition());
  }

  /**
   * Get replicas for topic.
   *
   * @param topic Topic of the requested replicas.
   * @return Replicas in this broker sharing the given topic.
   */
  public Collection<Replica> replicasOfTopicInBroker(String topic) {
    Map<Integer, Replica> topicReplicas = _topicReplicas.get(topic);

    return (topicReplicas == null) ? Collections.emptyList() : topicReplicas.values();
  }

  /**
   * Check broker liveness status.
   */
  public boolean isAlive() {
    return _state != State.DEAD;
  }

  /**
   * Check if the broker is a new broker
   */
  public boolean isNew() {
    return _state == State.NEW;
  }

  /**
   * Get the broker load of the broker.
   */
  public Load load() {
    return _load;
  }

  /**
   +   * The load for the replicas for which this broker is a leader.  This is meaningful for things like network bytes in,
   +   * but perhaps less meaningful for something like disk utilization.
   +   * @return
   +   */
  public Load leadershipLoad() {
    return _leadershipLoad;
  }

  /**
   * Get the set of topics in the broker.
   */
  public Set<String> topics() {
    return _topicReplicas.keySet();
  }

  /**
   * Sort replicas that needs to be sorted to balance the given resource by descending order of resource cost.
   * If resource is outbound network traffic, only leaders in the broker are sorted. Otherwise all replicas in
   * the broker are sorted.
   *
   * @param resource Type of the resource.
   * @return A list of sorted replicas chosen from the replicas residing in the given broker.
   */
  public List<Replica> sortedReplicas(Resource resource) {
    Set<Replica> candidateReplicas;
    // If a broker is already dead, we do not distinguish leader replica vs. non-leader replica anymore.
    if (resource.equals(Resource.NW_OUT) && isAlive()) {
      candidateReplicas = _leaderReplicas;
    } else {
      candidateReplicas = _replicas;
    }

    List<Replica> replicasToBeBalanced = new ArrayList<>();
    List<Replica> nativeReplicasToBeBalanced = new ArrayList<>();
    for (Replica replica : candidateReplicas) {
      if (_immigrantReplicas.contains(replica)) {
        replicasToBeBalanced.add(replica);
      } else {
        nativeReplicasToBeBalanced.add(replica);
      }
    }

    Collections.sort(replicasToBeBalanced,
                     (o1, o2) -> Double.compare(loadDensity(o2, resource),
                                                loadDensity(o1, resource)));
    Collections.sort(nativeReplicasToBeBalanced,
                     (o1, o2) -> Double.compare(loadDensity(o2, resource),
                                                loadDensity(o1, resource)));
    replicasToBeBalanced.addAll(nativeReplicasToBeBalanced);
    return replicasToBeBalanced;
  }

  /**
   * get the load density of a resource on a replica for sorting. This is to help reduce the movement cost.
   */
  private double loadDensity(Replica replica, Resource resource) {
    double expectedLoad = replica.load().expectedUtilizationFor(resource);
    if (expectedLoad == 0.0) {
      return 0.0;
    } else if (resource == Resource.DISK) {
      return expectedLoad;
    } else {
      double diskLoad = replica.load().expectedUtilizationFor(Resource.DISK);
      if (diskLoad == 0.0) {
        // Some big number
        return 1000000.0;
      } else {
        return expectedLoad / diskLoad;
      }
    }
  }

  /**
   * Set broker alive status.
   *
   * @param newState True if alive, false otherwise.
   */
  void setState(State newState) {
    _state = newState;
  }

  /**
   * Add replica to the broker.
   *
   * @param replica Replica to be added to the current broker.
   */
  void addReplica(Replica replica) {
    // Add replica to list of all replicas in the broker.
    if (_replicas.contains(replica)) {
      throw new IllegalStateException(String.format("Broker %d already has replica %s", _id, replica.topicPartition()));
    }
    _replicas.add(replica);

    if (replica.originalBroker().id() != _id) {
      _immigrantReplicas.add(replica);
    }

    // Add topic replica.
    Map<Integer, Replica> topicReplicas = _topicReplicas.get(replica.topicPartition().topic());
    if (topicReplicas == null) {
      topicReplicas = new HashMap<>();
      _topicReplicas.put(replica.topicPartition().topic(), topicReplicas);
    }
    topicReplicas.put(replica.topicPartition().partition(), replica);

    // Add leader replica.
    if (replica.isLeader()) {
      _leadershipLoad.addLoad(replica.load());
      _leaderReplicas.add(replica);
    }

    // Add replica load to the broker load.
    _load.addLoad(replica.load());
  }

  /**
   * (1) Make the replica with the given topicPartition and brokerId a follower.
   * (2) Remove and get the outbound network load associated with leadership from the given replica.
   *
   * @param topicPartition TopicPartition of the replica for which the outbound network load will be removed.
   * @return Leadership load by snapshot time.
   */
  Map<Resource, Map<Long, Double>> makeFollower(TopicPartition topicPartition) throws ModelInputException {
    Replica replica = replica(topicPartition);
    _leadershipLoad.subtractLoad(replica.load());

    Map<Resource, Map<Long, Double>> leadershipLoad = replica(topicPartition).makeFollower();
    // Remove leadership load from load.
    _load.subtractLoadFor(Resource.NW_OUT, leadershipLoad.get(Resource.NW_OUT));
    _load.subtractLoadFor(Resource.CPU, leadershipLoad.get(Resource.CPU));
    _leaderReplicas.remove(replica);
    return leadershipLoad;
  }

  /**
   * (1) Make the replica with the given topicPartition and brokerId the leader.
   * (2) Add the outbound network load associated with leadership to the given replica.
   *
   * @param topicPartition TopicPartition of the replica for which the outbound network load will be added.
   * @param leadershipLoad Leadership load to be added by snapshot time.
   */
  void makeLeader(TopicPartition topicPartition,
                  Map<Resource, Map<Long, Double>> leadershipLoad)
      throws ModelInputException {
    Replica replica = replica(topicPartition);
    replica.makeLeader(leadershipLoad);
    _leadershipLoad.addLoad(replica.load());
    // Add leadership load to load.
    _load.addLoadFor(Resource.NW_OUT, leadershipLoad.get(Resource.NW_OUT));
    _load.addLoadFor(Resource.CPU, leadershipLoad.get(Resource.CPU));
    _leaderReplicas.add(replica);
  }

  /**
   * Get the removed replica from the broker.
   *
   * @param topicPartition Topic partition of the replica to be removed from replicas in the current broker.
   * @return The removed replica or null if the topicPartition is not present.
   */
  Replica removeReplica(TopicPartition topicPartition) {
    // Find the index of the replica with the given replica ID and topic name.
    Replica removedReplica = replica(topicPartition);
    if (removedReplica != null) {
      // Remove the replica from the list of replicas.
      _replicas.remove(removedReplica);
      // Remove the load of the removed replica from the load of the broker.
      _load.subtractLoad(removedReplica.load());

      // Remove topic replica.
      Map<Integer, Replica> topicReplicas = _topicReplicas.get(topicPartition.topic());
      if (topicReplicas != null) {
        topicReplicas.remove(topicPartition.partition());
      }
      if (removedReplica.isLeader()) {
        _leadershipLoad.subtractLoad(removedReplica.load());
        _leaderReplicas.remove(removedReplica);
      }
      _immigrantReplicas.remove(removedReplica);
    }

    return removedReplica;
  }

  /**
   * Clear the content of monitoring data at each replica in the broker.
   */
  void clearLoad() {
    _replicas.forEach(Replica::clearLoad);
  }

  /**
   * Clear all replicas in the broker.
   */
  void clearReplicas() {
    _replicas.clear();
    _leaderReplicas.clear();
    _topicReplicas.clear();
    _immigrantReplicas.clear();
    _load.clearLoad();
    _leadershipLoad.clearLoad();
  }

  /**
   * Push the latest snapshot information containing the snapshot time and resource loads to the replica identified
   * by its topicPartition.
   *
   * @param topicPartition Topic partition that identifies the replica in this broker.
   * @param snapshot       Snapshot containing the latest state for each resource.
   * @throws ModelInputException
   */
  void pushLatestSnapshot(TopicPartition topicPartition, Snapshot snapshot)
      throws ModelInputException {
    Replica replica = replica(topicPartition);
    replica.pushLatestSnapshot(snapshot);
    if (replica.isLeader()) {
      _leadershipLoad.addSnapshot(snapshot);
    }
    _load.addSnapshot(snapshot);
  }

  /**
   * Output writing string representation of this class to the stream.
   * @param out the output stream.
   */
  public void writeTo(OutputStream out) throws IOException {
    String broker = String.format("<Broker id=\"%d\" state=\"%s\">%n", _id, _state);
    out.write(broker.getBytes(StandardCharsets.UTF_8));
    for (Replica replica : _replicas) {
      replica.writeTo(out);
    }
    out.write("</Broker>%n".getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public String toString() {
    return String.format("Broker[id=%d,rack=%s,state=%s,replicaCount=%d]", _id, rack().id(), _state, _replicas.size());
  }
}
