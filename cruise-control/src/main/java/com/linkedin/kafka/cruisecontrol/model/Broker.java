/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.kafka.common.TopicPartition;


/**
 * A class that holds the information of the broker, including its liveness and load for replicas. A broker object is
 * created as part of a rack structure.
 */
public class Broker implements Serializable, Comparable<Broker> {

  public enum State {
    ALIVE, DEAD, NEW, DEMOTED
  }

  private final int _id;
  private final Host _host;
  private final double[] _brokerCapacity;
  private final Set<Replica> _replicas;
  private final Set<Replica> _leaderReplicas;
  /** A map of cached sorted replicas using different user defined score functions. */
  private final Map<String, SortedReplicas> _sortedReplicas;
  /** Set of immigrant replicas */
  private final Set<Replica> _immigrantReplicas;
  /** A map for tracking topic -&gt; (partitionId -&gt; replica). */
  private final Map<String, Map<Integer, Replica>> _topicReplicas;
  private final Load _load;
  private final Load _leadershipLoadForNwResources;
  private State _state;

  /**
   * Constructor for Broker class.
   *
   * @param host           The host this broker is on
   * @param id             The id of the broker.
   * @param brokerCapacity The capacity of the broker.
   */
  Broker(Host host, int id, Map<Resource, Double> brokerCapacity) {
    if (brokerCapacity == null) {
      throw new IllegalArgumentException("Attempt to create broker " + id + " on host " + host.name() + " with null capacity.");
    }
    _host = host;
    _id = id;
    _brokerCapacity = new double[Resource.cachedValues().size()];
    for (Map.Entry<Resource, Double> entry : brokerCapacity.entrySet()) {
      _brokerCapacity[entry.getKey().id()] = entry.getValue();
    }
    _replicas = new HashSet<>();
    _leaderReplicas = new HashSet<>();
    _topicReplicas = new HashMap<>();
    _sortedReplicas = new HashMap<>();
    _immigrantReplicas = new HashSet<>();
    // Initially broker does not contain any load.
    _load = new Load();
    _leadershipLoadForNwResources = new Load();
    _state = State.ALIVE;
  }

  public Host host() {
    return _host;
  }

  public State getState() {
    return _state;
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
    return Collections.unmodifiableSet(_replicas);
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
   * @param tp Topic partition of the replica.
   * @return Replica if it exists in the broker, null otherwise.
   */
  public Replica replica(TopicPartition tp) {
    Map<Integer, Replica> topicReplicas = _topicReplicas.get(tp.topic());
    if (topicReplicas == null) {
      return null;
    }

    return topicReplicas.get(tp.partition());
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
   * Check if the broker is demoted from being a partition leader.
   */
  public boolean isDemoted() {
    return _state == State.DEMOTED;
  }

  /**
   * Get the broker load of the broker.
   */
  public Load load() {
    return _load;
  }

  /**
   * The load for the replicas for which this broker is a leader. This is meaningful for network bytes in, and
   * network bytes out but not meaningful for the other resources.
   */
  public Load leadershipLoadForNwResources() {
    return _leadershipLoadForNwResources;
  }

  /**
   * Get the set of topics in the broker.
   */
  public Set<String> topics() {
    return _topicReplicas.keySet();
  }

  /**
   * Get the tracked sorted replicas using the given sort name.
   *
   * @param sortName the sort name.
   * @return the {@link SortedReplicas} for the given sort name.
   */
  public SortedReplicas trackedSortedReplicas(String sortName) {
    SortedReplicas sortedReplicas = _sortedReplicas.get(sortName);
    if (sortedReplicas == null) {
      throw new IllegalStateException("The sort name " + sortName + "  is not found. Make sure trackSortedReplicas() " +
                                          "has been called for the sort name");
    }
    return sortedReplicas;
  }

  /**
   * Get a comparator for the replicas in the broker. The comparisons performed are:
   * 1. immigrant replicas has higher priority, i.e. comes before the native replicas.
   * 2. the replicas with lower resource usage comes before those with higher resource usage.
   *
   * @param resource the resource for the comparator to use.
   * @return a Comparator to compare the replicas for the given resource.
   */
  public Comparator<Replica> replicaComparator(Resource resource) {
    return (r1, r2) -> {
      boolean isR1Immigrant = _immigrantReplicas.contains(r1);
      boolean isR2Immigrant = _immigrantReplicas.contains(r2);
      if (isR1Immigrant && !isR2Immigrant) {
        return -1;
      } else if (!isR1Immigrant && isR2Immigrant) {
        return 1;
      } else {
        int result = Double.compare(r1.load().expectedUtilizationFor(resource),
                                    r2.load().expectedUtilizationFor(resource));
        return result != 0 ? result : r1.compareTo(r2);
      }
    };
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
      _leadershipLoadForNwResources.addLoad(replica.load());
      _leaderReplicas.add(replica);
    }

    // Add replica load to the broker load.
    _load.addLoad(replica.load());
    _sortedReplicas.values().forEach(sr -> sr.add(replica));
  }

  /**
   * Track the sorted replicas using the given score function. The sort first uses the priority function to
   * sort the replicas, then use the score function to sort the replicas. The priority function is useful
   * to priorities a particular type of replicas, e.g leader replicas, immigrant replicas, etc.
   *
   * @param sortName the name of the tracked sorted replicas.
   * @param selectionFunc the selection function to decide which replicas to include.
   * @param priorityFunc the priority function to sort replicas.
   * @param scoreFunc the score function to sort replicas.
   */
  void trackSortedReplicas(String sortName,
                           Function<Replica, Boolean> selectionFunc,
                           Function<Replica, Integer> priorityFunc,
                           Function<Replica, Double> scoreFunc) {
    _sortedReplicas.putIfAbsent(sortName, new SortedReplicas(this, selectionFunc, priorityFunc, scoreFunc));
  }

  /**
   * Untrack the sorted replicas for the given sort name. This helps release memory.
   *
   * @param sortName the name of the tracked sorted replicas.
   */
  void untrackSortedReplicas(String sortName) {
    _sortedReplicas.remove(sortName);
  }

  private void updateSortedReplicas(Replica replica) {
    _sortedReplicas.values().forEach(sr -> {
      sr.remove(replica);
      sr.add(replica);
    });
  }

  /**
   * (1) Make the replica with the given topic partition and brokerId a follower.
   * (2) Remove and get the outbound network load associated with leadership from the given replica.
   * (3) Remove and get the CPU load associated with leadership from the given replica.
   *
   * @param tp TopicPartition of the replica for which the outbound network load will be removed.
   * @return Leadership load by snapshot time.
   */
  AggregatedMetricValues makeFollower(TopicPartition tp) {
    Replica replica = replica(tp);
    _leadershipLoadForNwResources.subtractLoad(replica.load());

    AggregatedMetricValues leadershipLoadDelta = replica.makeFollower();
    // Remove leadership load from load.
    _load.subtractLoad(leadershipLoadDelta);
    _leaderReplicas.remove(replica);
    updateSortedReplicas(replica);
    return leadershipLoadDelta;
  }

  /**
   * (1) Make the replica with the given topic partition and brokerId the leader.
   * (2) Add the outbound network load associated with leadership to the given replica.
   * (3) Add the CPU load associated with leadership.
   *
   * @param tp TopicPartition of the replica for which the outbound network load will be added.
   * @param leadershipLoadDelta Resource to leadership load to be added by snapshot time.
   */
  void makeLeader(TopicPartition tp, AggregatedMetricValues leadershipLoadDelta) {
    Replica replica = replica(tp);
    replica.makeLeader(leadershipLoadDelta);
    _leadershipLoadForNwResources.addLoad(replica.load());
    // Add leadership load to load.
    _load.addLoad(leadershipLoadDelta);
    _leaderReplicas.add(replica);
    updateSortedReplicas(replica);
  }

  /**
   * Get the removed replica from the broker.
   *
   * @param tp Topic partition of the replica to be removed from replicas in the current broker.
   * @return The removed replica or null if the topic partition is not present.
   */
  Replica removeReplica(TopicPartition tp) {
    // Find the index of the replica with the given replica ID and topic name.
    Replica removedReplica = replica(tp);
    if (removedReplica != null) {
      // Remove the replica from the list of replicas.
      _replicas.remove(removedReplica);
      // Remove the load of the removed replica from the load of the broker.
      _load.subtractLoad(removedReplica.load());

      // Remove topic replica.
      Map<Integer, Replica> topicReplicas = _topicReplicas.get(tp.topic());
      if (topicReplicas != null) {
        topicReplicas.remove(tp.partition());
      }
      if (removedReplica.isLeader()) {
        _leadershipLoadForNwResources.subtractLoad(removedReplica.load());
        _leaderReplicas.remove(removedReplica);
      }
      _immigrantReplicas.remove(removedReplica);
      _sortedReplicas.values().forEach(sr -> sr.remove(removedReplica));
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
    _leadershipLoadForNwResources.clearLoad();
  }

  /**
   * Set the load of the replicas. The load will be added to the broker load. Note that this method should only
   * be called once for each replica.
   *
   * @param tp Topic partition that identifies the replica in this broker.
   * @param aggregatedMetricValues The metric values of this topic partition.
   * @param windows The windows list of the aggregated metric values.
   */
  void setReplicaLoad(TopicPartition tp, AggregatedMetricValues aggregatedMetricValues, List<Long> windows) {
    Replica replica = replica(tp);
    replica.setMetricValues(aggregatedMetricValues, windows);
    if (replica.isLeader()) {
      _leadershipLoadForNwResources.addMetricValues(aggregatedMetricValues, windows);
    }
    _load.addMetricValues(aggregatedMetricValues, windows);
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    List<Map<String, Object>> replicaList = new ArrayList<>();
    for (Replica replica : _replicas) {
      replicaList.add(replica.getJsonStructureForLoad());
    }
    Map<String, Object> brokerMap = new HashMap<>();
    brokerMap.put("brokerid", _id);
    brokerMap.put("brokerstate", _state);
    brokerMap.put("replicas", replicaList);
    return brokerMap;
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

  /**
   * Compare by broker id.
   */
  @Override
  public int compareTo(Broker o) {
    return Integer.compare(_id, o.id());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Broker broker = (Broker) o;
    return _id == broker._id;
  }

  @Override
  public int hashCode() {
    return _id;
  }
}
