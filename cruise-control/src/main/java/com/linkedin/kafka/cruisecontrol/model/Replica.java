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
import java.util.HashMap;
import java.util.Map;

import java.util.Objects;
import org.apache.kafka.common.TopicPartition;

/**
 * A class that holds the information of the replica, including its load, leader, topic partition, and broker. A replica
 * object is created as part of a broker structure.
 */
public class Replica implements Serializable, Comparable<Replica> {
  private final TopicPartition _tp;
  private final Load _load;
  private final Broker _originalBroker;
  private Broker _broker;
  private boolean _isLeader;

  /**
   * A constructor for a replica.
   *
   * @param tp Topic partition information of the replica.
   * @param broker         The broker of the replica.
   * @param isLeader       A flag to represent whether the replica is the isLeader or not.
   */
  Replica(TopicPartition tp, Broker broker, boolean isLeader) {
    _tp = tp;
    _load = Load.newLoad();
    _originalBroker = broker;
    _broker = broker;
    _isLeader = isLeader;
  }

  /**
   * Get the topic partition.
   */
  public TopicPartition topicPartition() {
    return _tp;
  }

  /**
   * Get the replica load for each resource. Replicas always have an associated load.
   */
  public Load load() {
    return _load;
  }

  /**
   * Get the original broker of this replica before rebalance.
   */
  public Broker originalBroker() {
    return _originalBroker;
  }

  /**
   * Get broker that the replica resides in.
   */
  public Broker broker() {
    return _broker;
  }

  /**
   * Check the leadership status of the broker.
   */
  public boolean isLeader() {
    return _isLeader;
  }

  /**
   * Set broker that the replica resides in.
   *
   * @param broker Broker that the replica resides in.
   */
  void setBroker(Broker broker) {
    _broker = broker;
  }

  /**
   * Set Leadership status of the broker
   *
   * @param leader True if leader, false otherwise.
   */
  void setLeadership(boolean leader) {
    _isLeader = leader;
  }

  /**
   * Pushes the latest snapshot information containing the snapshot time and resource loads for the replica.
   *
   * @param snapshot Snapshot containing latest state for each resource.
   */
  void pushLatestSnapshot(Snapshot snapshot) throws ModelInputException {
    _load.pushLatestSnapshot(snapshot);
  }

  /**
   * Clear the content of monitoring data at each replica in the broker.
   */
  void clearLoad() {
    _load.clearLoad();
  }

  /**
   * (1) Remove leadership from the replica.
   * (2) Clear and get the outbound network load associated with leadership from the given replica.
   * (3) Clear and get the CPU leadership load associated with leadership from the given replica.
   *
   * @return Removed leadership load by snapshot time -- i.e. outbound network and fraction of CPU load by snapshot time.
   */
  Map<Resource, Map<Long, Double>> makeFollower()
      throws ModelInputException {
    // Remove leadership from the replica.
    setLeadership(false);
    // Clear and get the outbound network and CPU load associated with leadership from the given replica.
    Map<Long, Double> leadershipNwOutLoad = _load.loadFor(Resource.NW_OUT);
    Map<Long, Double> leaderCpuLoad = _load.loadFor(Resource.CPU);

    // Remove the outbound network leadership load from replica.
    _load.clearLoadFor(Resource.NW_OUT);

    // Remove the CPU leadership load from replica.
    Map<Long, Double> followerCpuLoad = new HashMap<>();
    Map<Long, Double> leadershipCpuLoad = new HashMap<>();
    leaderCpuLoad.forEach((k, v) -> {
      double newCpuUtilization = ModelUtils.getFollowerCpuUtilFromLeaderLoad(_load, k);

      followerCpuLoad.put(k, newCpuUtilization);
      leadershipCpuLoad.put(k, v - newCpuUtilization);
    });
    _load.setLoadFor(Resource.CPU, followerCpuLoad);

    // Get the change of the load for upper layer.
    Map<Resource, Map<Long, Double>> leadershipLoad = new HashMap<>();
    leadershipLoad.put(Resource.NW_OUT, leadershipNwOutLoad);
    leadershipLoad.put(Resource.CPU, leadershipCpuLoad);

    // Return removed leadership load.
    return leadershipLoad;
  }

  /**
   * (1) Add leadership to the replica.
   * (2) Set the outbound network load associated with leadership.
   * (3) Add the CPU load associated with leadership.
   *
   * @param resourceToLeadershipLoadBySnapshotTime Resource to leadership load to be added by snapshot time.
   */
  void makeLeader(Map<Resource, Map<Long, Double>> resourceToLeadershipLoadBySnapshotTime) throws ModelInputException {
    // Add leadership to the replica.
    setLeadership(true);
    _load.setLoadFor(Resource.NW_OUT, resourceToLeadershipLoadBySnapshotTime.get(Resource.NW_OUT));
    _load.addLoadFor(Resource.CPU, resourceToLeadershipLoadBySnapshotTime.get(Resource.CPU));
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> replicaMap = new HashMap<>();
    replicaMap.put("isLeader", _isLeader);
    replicaMap.put("broker", _broker.id());
    replicaMap.put("topic", _tp.topic());
    replicaMap.put("partition", _tp.partition());
    replicaMap.put("originalBroker", _originalBroker == null ? -1 : _originalBroker.id());
    return replicaMap;
  }

  /*
   * Return an object that can be further used
   * to encode into JSON (version 2 used in load)
   */
  public Map<String, Object> getJsonStructureForLoad() {
    Map<String, Object> replicaMap = new HashMap<>();
    replicaMap.put("isLeader", _isLeader);
    replicaMap.put("brokerid", _broker.id());
    replicaMap.put("topic", _tp.topic());
    replicaMap.put("partition", _tp.partition());
    replicaMap.put("load", _load.getJsonStructure());
    return replicaMap;
  }

  /**
   * Output writing string representation of this class to the stream.
   * @param out the output stream.
   */
  public void writeTo(OutputStream out) throws IOException {
    out.write(String.format("<Replica isLeader=\"%s\" id=\"%d\">%n%s", isLeader(), _broker.id(), _tp).getBytes(StandardCharsets.UTF_8));
    _load.writeTo(out);
    out.write("</Replica>%n".getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Get string representation of Replica in XML format.
   */
  @Override
  public String toString() {
    return String.format("Replica[isLeader=%s,rack=%s,broker=%d,TopicPartition=%s,origBroker=%d]", _isLeader,
                         _broker.rack().id(), _broker.id(), _tp,
                         _originalBroker == null ? -1 : _originalBroker.id());
  }

  /**
   * Compare (1) by partition id then (2) by broker id then (3) by topic name.
   */
  @Override
  public int compareTo(Replica o) {
    // Primary sort: by partition id.
    if (_tp.partition() > o.topicPartition().partition()) {
      return 1;
    } else if (_tp.partition() < o.topicPartition().partition()) {
      return -1;
    }

    // Secondary sort: by broker id.
    if (_broker.id() > o.broker().id()) {
      return 1;
    } else if (_broker.id() < o.broker().id()) {
      return -1;
    }

    // Final sort: by topic name.
    return _tp.topic().compareTo(o.topicPartition().topic());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Replica replica = (Replica) o;
    return Objects.equals(_tp, replica._tp) && _broker.id() == replica._broker.id();
  }

  @Override
  public int hashCode() {
    return Objects.hash(_tp, _broker.id());
  }
}
