/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.common.TopicPartition;
import java.util.stream.Collectors;

/**
 * A class that holds the disk information of a broker, including its liveness, capacity and load. It is created as part
 * of a broker structure. A disk object represent an independent disk Kafka broker uses to host data and is managed by
 * Kafka LogManager.
 *
 */
public class Disk implements Comparable<Disk> {
  private static final double DEAD_DISK_CAPACITY = -1.0;

  public enum State {
    ALIVE, DEAD, DEMOTED
  }

  private final String _logDir;
  private double _capacity;
  private final Set<Replica> _replicas;
  private State _state;
  private final Broker _broker;
  // Utilization is only relevant for alive disk.
  private double _utilization;
  // A map of cached sorted replicas using different user defined score functions.
  private final Map<String, SortedReplicas> _sortedReplicas;

  /**
   * Constructor for Disk class.
   *
   * @param logDir         The log directory maps to disk.
   * @param broker         The broker of the disk.
   * @param diskCapacity   The capacity of the disk. If disk is dead, the capacity value is expected to be negative.
   */
  Disk(String logDir, Broker broker, double diskCapacity) {
    _logDir = logDir;
    _broker = broker;
    _replicas = new HashSet<>();
    _utilization = 0;
    _sortedReplicas = new HashMap<>();

    if (diskCapacity < 0) {
      _capacity = DEAD_DISK_CAPACITY;
      _state = State.DEAD;
    } else {
      _capacity = diskCapacity;
      _state = State.ALIVE;
    }
  }

  public String logDir() {
    return _logDir;
  }

  public double capacity() {
    return _capacity;
  }

  public Disk.State state() {
    return _state;
  }

  public boolean isAlive() {
    return _state == State.ALIVE;
  }

  public Set<Replica> replicas() {
    return Collections.unmodifiableSet(_replicas);
  }

  public Set<Replica> leaderReplicas() {
    return Collections.unmodifiableSet(_replicas.stream().filter(Replica::isLeader).collect(Collectors.toSet()));
  }

  public Broker broker() {
    return _broker;
  }

  public double utilization() {
    return _utilization;
  }

  /**
   * Set Disk status.
   *
   * @param newState The new state of the broker.
   */
  public void setState(Disk.State newState) {
    _state = newState;
    if (_state == State.DEAD) {
      _capacity = DEAD_DISK_CAPACITY;
    }
  }

  /**
   * Add replica to the disk.
   *
   * @param replica Replica to be added to the current disk.
   */
  void addReplica(Replica replica) {
    if (_replicas.contains(replica)) {
      throw new IllegalStateException(String.format("Disk %s already has replica %s", _logDir, replica.topicPartition()));
    }
    _utilization += replica.load().expectedUtilizationFor(Resource.DISK);
    _replicas.add(replica);
    replica.setDisk(this);
    _sortedReplicas.values().forEach(sr -> sr.add(replica));
  }

  /**
   * Add replica load to the disk. This is used in cluster model initialization. When first adding replica to disk in
   * {@link ClusterModel#createReplica(String, int, TopicPartition, int, boolean, boolean, String)}, the replica is load
   * is not initiated, therefore later in
   * {@link ClusterModel#setReplicaLoad(String, int, TopicPartition, AggregatedMetricValues, List)}, disk needs to refresh
   * its utilization with initiated replica load.
   *
   * @param replica The replica whose load is initiated.
   */
  void addReplicaLoad(Replica replica) {
    _utilization += replica.load().expectedUtilizationFor(Resource.DISK);
  }

  /**
   * Remove replica from the disk.
   *
   * @param replica Replica to be removed from the current disk.
   */
  void removeReplica(Replica replica) {
    if (!_replicas.contains(replica)) {
      throw new IllegalStateException(String.format("Disk %s does not has replica %s", _logDir, replica.topicPartition()));
    }
    _utilization -= replica.load().expectedUtilizationFor(Resource.DISK);
    _replicas.remove(replica);
    _sortedReplicas.values().forEach(sr -> sr.remove(replica));
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
  public void trackSortedReplicas(String sortName,
                                  Function<Replica, Boolean> selectionFunc,
                                  Function<Replica, Integer> priorityFunc,
                                  Function<Replica, Double> scoreFunc) {
    _sortedReplicas.putIfAbsent(sortName, new SortedReplicas(_broker, this, selectionFunc, priorityFunc, scoreFunc, true));
  }

  public void trackSortedReplicas(String sortName,
                                  Function<Replica, Boolean> selectionFunc,
                                  Function<Replica, Double> scoreFunc) {
    _sortedReplicas.putIfAbsent(sortName, new SortedReplicas(_broker, this, selectionFunc, (r1) -> 0, scoreFunc, true));
  }

  /**
   * Untrack the sorted replicas for the given sort name. This helps release memory.
   *
   * @param sortName the name of the tracked sorted replicas.
   */
  public void untrackSortedReplicas(String sortName) {
    _sortedReplicas.remove(sortName);
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

  @Override
  public int compareTo(Disk d) {
    int result = _broker.compareTo(d.broker());
    if (result == 0) {
      return _logDir.compareTo(d.logDir());
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof  Disk)) {
      return false;
    }
    return compareTo((Disk) o) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_broker, _logDir);
  }

  /**
   * Output writing string representation of this class to the stream.
   * @param out the output stream.
   */
  public void writeTo(OutputStream out) throws IOException {
    String disk = String.format("<Disk logdir=\"%s\" state=\"%s\">%n", _logDir, _state);
    out.write(disk.getBytes(StandardCharsets.UTF_8));
    for (Replica replica : _replicas) {
      replica.writeTo(out);
    }
    out.write("</Disk>%n".getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public String toString() {
    return String.format("Disk[logdir=%s,state=%s,capacity=%f,replicaCount=%d]", _logDir, _state, _capacity, _replicas.size());
  }

  public DiskStats diskStats() {
    return new DiskStats((int) _replicas.stream().filter(Replica::isLeader).count(),
                          _replicas.size(),
                          _utilization,
                          _capacity);
  }
}
