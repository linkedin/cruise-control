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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;


/**
 * A class that holds the information of the rack, including its topology, liveness and load for brokers, and
 * replicas. A rack object is created as part of a cluster structure.
 */
public class Rack implements Serializable {
  private static final long serialVersionUID = 6866290448556002509L;
  private final String _id;
  private final Map<String, Host> _hosts;
  private final Map<Integer, Broker> _brokers;
  private Load _load;
  private final double[] _rackCapacity;

  /**
   * Constructor of the rack class using the given id to identify the rack.
   *
   * @param id An identifier for this rack.
   */
  Rack(String id) {
    _id = id;
    _hosts = new HashMap<>();
    _brokers = new HashMap<>();
    // Initially rack does not contain any load -- cannot create a load with a specific window size.
    _load = Load.newLoad();
    _rackCapacity = new double[Resource.values().length];
  }

  /**
   * Get the rack load information.
   */
  public Load load() {
    return _load;
  }

  /**
   * Get the rack Id.
   */
  public String id() {
    return _id;
  }

  /**
   * Get the collection of brokers in the current rack.
   */
  public Collection<Broker> brokers() {
    return _brokers.values();
  }

  /**
   * Return the hosts in this rack.
   */
  public Collection<Host> hosts() {
    return _hosts.values();
  }

  /**
   * Get the broker with the given broker id.
   *
   * @param brokerId Id of the queried broker.
   * @return The broker with the id if it is found in the rack; null otherwise.
   */
  public Broker broker(int brokerId) {
    return _brokers.get(brokerId);
  }

  /**
   * Get the list of replicas in the rack.
   */
  public List<Replica> replicas() {
    List<Replica> replicas = new ArrayList<>();

    for (Host host : _hosts.values()) {
      replicas.addAll(host.replicas());
    }
    return replicas;
  }

  /**
   * Get the number of replicas with the given topic name in this rack.
   *
   * @param topic Name of the topic for which the number of replicas in this rack will be counted.
   * @return Number of replicas with the given topic name in this rack.
   */
  public int numTopicReplicas(String topic) {
    int numTopicReplicas = 0;

    for (Host host : _hosts.values()) {
      numTopicReplicas += host.numTopicReplicas(topic);
    }
    return numTopicReplicas;
  }

  /**
   * Get a set of topic names in the cluster.
   */
  public Set<String> topics() {
    Set<String> topics = new HashSet<>();

    for (Broker broker : _brokers.values()) {
      topics.addAll(broker.topics());
    }
    return topics;
  }

  /**
   * Get rack capacity for the requested resource. Rack capacity represents the total capacity of the live
   * brokers in the rack for the requested resource.
   *
   * @param resource Resource for which capacity will be provided.
   * @return Healthy rack capacity of the resource.
   */
  public double capacityFor(Resource resource) {
    return _rackCapacity[resource.id()];
  }

  /**
   * Checks if rack has at least one alive servers. If none of the servers is alive, rack is considered dead.
   *
   * @return True if rack is alive, false otherwise.
   */
  public boolean isRackAlive() {
    for (Host host : _hosts.values()) {
      if (host.isAlive()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the removed replica from the rack.
   *
   * @param brokerId       Id of the broker containing the
   * @param topicPartition Topic partition of the replica to be removed.
   * @return The requested replica if the id exists in the rack and the partition is found in the broker, and
   * null otherwise.
   */
  Replica removeReplica(int brokerId, TopicPartition topicPartition) {
    Broker broker = _brokers.get(brokerId);
    if (broker != null) {
      // Remove the replica and the associated load from the broker that it resides in.
      Replica removedReplica = broker.host().removeReplica(brokerId, topicPartition);
      // Remove the load of the removed replica from the recent load of the rack.
      _load.subtractLoad(removedReplica.load());
      // Return the removed replica.
      return removedReplica;
    }

    return null;
  }

  /**
   * Add this replica and related load to the destination broker / destination rack.
   *
   * @param replica Replica to be added to the cluster.
   */
  void addReplica(Replica replica) {
    replica.broker().host().addReplica(replica);
    // Add replica load to the recent rack load.
    _load.addLoad(replica.load());
  }

  /**
   * (1) Make the replica with the given topicPartition and brokerId a follower.
   * (2) Remove and get the outbound network load associated with leadership from the given replica.
   *
   * @param brokerId       Id of the broker containing the replica.
   * @param topicPartition TopicPartition of the replica for which the outbound network load will be removed.
   * @return Leadership load by snapshot time.
   */
  Map<Resource, Map<Long, Double>> makeFollower(int brokerId,
                                                TopicPartition topicPartition) throws ModelInputException {
    Host host = _brokers.get(brokerId).host();
    Map<Resource, Map<Long, Double>> leadershipLoad = host.makeFollower(brokerId, topicPartition);
    // Remove leadership load from recent load.
    _load.subtractLoadFor(Resource.NW_OUT, leadershipLoad.get(Resource.NW_OUT));
    _load.subtractLoadFor(Resource.CPU, leadershipLoad.get(Resource.CPU));
    return leadershipLoad;
  }

  /**
   * (1) Make the replica with the given topicPartition and brokerId the leader.
   * (2) Add the outbound network load associated with leadership to the given replica.
   *
   * @param brokerId                     Id of the broker containing the replica.
   * @param topicPartition               TopicPartition of the replica for which the outbound network load will be added.
   * @param leadershipLoadBySnapshotTime Leadership load to be added by snapshot time.
   */
  void makeLeader(int brokerId,
                  TopicPartition topicPartition,
                  Map<Resource, Map<Long, Double>> leadershipLoadBySnapshotTime)
      throws ModelInputException {
    Host host = _brokers.get(brokerId).host();
    host.makeLeader(brokerId, topicPartition, leadershipLoadBySnapshotTime);
    // Add leadership load to recent load.
    _load.addLoadFor(Resource.NW_OUT, leadershipLoadBySnapshotTime.get(Resource.NW_OUT));
    _load.addLoadFor(Resource.CPU, leadershipLoadBySnapshotTime.get(Resource.CPU));
  }

  /**
   * Clear the content of monitoring data at each replica in the rack.
   * Typically, if a change is detected in topology, this method is called to clear the monitoring data collected
   * with the old topology.
   */
  void clearLoad() {
    _hosts.values().forEach(Host::clearLoad);
    _load.clearLoad();
  }

  /**
   * Pushes the latest snapshot information containing the snapshot time and resource loads to the rack.
   *
   * @param brokerId       Broker Id containing the replica with the given topic partition.
   * @param topicPartition Topic partition that identifies the replica in this broker.
   * @param snapshot       Snapshot containing latest state for each resource.
   */
  void pushLatestSnapshot(int brokerId, TopicPartition topicPartition, Snapshot snapshot)
      throws ModelInputException {
    Host host = _brokers.get(brokerId).host();
    host.pushLatestSnapshot(brokerId, topicPartition, snapshot);
    // Update the recent load of this rack.
    _load.addSnapshot(snapshot);
  }

  /**
   * Create a broker under this rack, and get the created broker.
   *
   * @param brokerId       Id of the broker to be created.
   * @param hostName           The hostName of the broker
   * @param brokerCapacity Capacity of the created broker.
   * @return Created broker.
   */
  Broker createBroker(int brokerId, String hostName, Map<Resource, Double> brokerCapacity) {
    Host host = _hosts.computeIfAbsent(hostName, name -> new Host(name, this));
    Broker broker = host.createBroker(brokerId, brokerCapacity);
    _brokers.put(brokerId, broker);
    for (Map.Entry<Resource, Double> entry : brokerCapacity.entrySet()) {
      _rackCapacity[entry.getKey().id()] += entry.getValue();
    }
    return broker;
  }

  /**
   * Set the broker state and update the capacity
   */
  void setBrokerState(int brokerId, Broker.State newState) {
    // Broker is dead
    Broker broker = broker(brokerId);
    broker.host().setBrokerState(brokerId, newState);
    for (Resource r : Resource.cachedValues()) {
      double capacity = 0;
      for (Host h : _hosts.values()) {
        capacity += h.capacityFor(r);
      }
      _rackCapacity[r.id()] = capacity;
    }
  }

  /**
   * Output writing string representation of this class to the stream.
   * @param out the output stream.
   */
  public void writeTo(OutputStream out) throws IOException {
    String rack = String.format("<Rack id=\"%s\">%n", _id);
    out.write(rack.getBytes(StandardCharsets.UTF_8));
    for (Host host : _hosts.values()) {
      host.writeTo(out);
    }
    out.write("</Rack>%n".getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Get string representation of Rack in XML format.
   */
  public String toXml() {
    StringBuilder rack = new StringBuilder().append(String.format("<Rack id=\"%s\">%n", _id));

    for (Host host : _hosts.values()) {
      rack.append(host.toString());
    }

    return rack + "</Rack>%n";
  }

  @Override
  public String toString() {
    return "Rack{" + "_id=\"" + _id + "\", _hosts=" + _hosts.size() + ", _brokers=" + _brokers.size() + ", _load=" + _load + '}';
  }
}
