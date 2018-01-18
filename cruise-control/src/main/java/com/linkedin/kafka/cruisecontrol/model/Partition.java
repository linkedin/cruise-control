/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;


/**
 * A partition consists of replicas with the same topic partition. One replica is the leader, the rest of the replicas
 * are followers.
 */
public class Partition implements Serializable {
  private final TopicPartition _tp;
  private final List<Replica> _followers;
  private Replica _leader;

  /**
   * Constructor for Partition class.
   *
   * @param tp Topic partition information for the replica in this partition,
   * @param leader         Leader of the replica in this partition,
   * @throws ModelInputException
   */
  Partition(TopicPartition tp, Replica leader) throws ModelInputException {
    if (leader != null && !leader.isLeader()) {
      throw new ModelInputException("Inconsistent leadership information. Specified leader replica " + leader +
                                    " is not marked as leader.");
    }
    _tp = tp;
    _followers = new ArrayList<>();
    _leader = leader;
  }

  /**
   * Add follower to the partition.
   *
   * @param follower Follower replica.
   * @throws ModelInputException
   */
  void addFollower(Replica follower) throws ModelInputException {
    addFollower(-1, follower);
  }

  /**
   * Add follower to the partition in the given index determining the follower position in replica list.
   * @param index An index determining the follower position in replica list.
   * @param follower Follower replica.
   * @throws ModelInputException
   */
  private void addFollower(int index, Replica follower) throws ModelInputException {
    if (follower.isLeader()) {
      throw new ModelInputException("Inconsistent leadership information. Trying to add follower replica " +
                                    follower + " while it is a leader.");
    }
    if (!follower.topicPartition().equals(_tp)) {
      throw new ModelInputException("Inconsistent topic partition. Trying to add follower replica " + follower +
                                    " to partition " + _tp + ".");
    }
    // Add follower to the list of followers.
    if (index >= 0) {
      _followers.add(index, follower);
    } else {
      _followers.add(follower);
    }
  }

  /**
   * Get follower replicas.
   */
  public List<Replica> followers() {
    return _followers;
  }

  /**
   * Get the leader replica.
   */
  public Replica leader() {
    return _leader;
  }

  /**
   * Get replica with the given broker id in this partition.
   *
   * @param brokerId Broker id of the requested replica in this partition.
   * @return Replica with the given broker id in this partition.
   * @throws ModelInputException
   */
  Replica replica(long brokerId)
      throws ModelInputException {
    if (_leader.broker().id() == brokerId) {
      return _leader;
    }
    for (Replica follower : _followers) {
      if (follower.broker().id() == brokerId) {
        return follower;
      }
    }

    throw new ModelInputException("Requested replica " + brokerId + " is not a replica of partition " + _tp);
  }

  /**
   * Get the set of brokers that followers reside in.
   */
  public List<Broker> followerBrokers() {
    return _followers.stream().map(Replica::broker).collect(Collectors.toList());
  }

  /**
   * Given two follower indices in the follower list, swap their positions.
   *
   * @param followerIndex The index of the first follower to be swapped.
   * @param otherFollowerIndex The index of the second follower to be swapped
   */
  public void swapFollowerPositions(int followerIndex, int otherFollowerIndex) {
    Replica follower1 = _followers.get(followerIndex);
    Replica follower2 = _followers.get(otherFollowerIndex);

    _followers.set(otherFollowerIndex, follower1);
    _followers.set(followerIndex, follower2);
  }

  /**
   * Get the set of brokers that contain replicas of the partition.
   */
  public Set<Broker> partitionBrokers() {
    Set<Broker> partitionBrokers = new HashSet<>();
    // Add leader and follower brokers.
    partitionBrokers.add(_leader.broker());
    _followers.forEach(r -> partitionBrokers.add(r.broker()));
    return partitionBrokers;
  }

  /**
   * @return the set of racks that contains replicas of the partition.
   */
  public Set<Rack> partitionRacks() {
    Set<Rack> partitionRacks = new HashSet<>();
    partitionRacks.add(_leader.broker().rack());
    _followers.forEach(r -> partitionRacks.add(r.broker().rack()));
    return partitionRacks;
  }

  /**
   * Set the leader to the value specified by the leader parameter.
   *
   * @param leader Leader replica of partition.
   * @throws ModelInputException
   */
  void setLeader(Replica leader)
      throws ModelInputException {
    if (!leader.isLeader()) {
      throw new ModelInputException("Inconsistent leadership information. Trying to set " + leader.broker() +
                                    " as the leader for partition " + _tp + " while the replica is not marked as a leader.");
    }
    _leader = leader;
  }

  /**
   * Relocate leadership by:
   * (1) Remove the prospective leader from followers,
   * (2) make the old leader a follower, and
   * (3) make the prospective leader the leader.
   *
   * @param prospectiveLeader Prospective leader.
   */
  void relocateLeadership(Replica prospectiveLeader) throws ModelInputException {
    // Remove prospective leader from followers.
    int followerPosition = _followers.indexOf(prospectiveLeader);
    _followers.remove(prospectiveLeader);
    // Make the old leader a follower.
    addFollower(followerPosition, _leader);
    // Make the prospective leader the leader.
    _leader = prospectiveLeader;
  }

  /**
   * Clear the leader to null and clear followers.
   */
  public void clear() {
    _followers.clear();
    _leader = null;
  }

  /**
   * Get string representation of Partition in XML format.
   */
  @Override
  public String toString() {
    StringBuilder partition = new StringBuilder().append(String.format("<Partition>%n<Leader>%s</Leader>%n", _leader));

    for (Replica follower : _followers) {
      partition.append(String.format("<Follower>%s</Follower>%n", follower));
    }
    return partition.append("</Partition>%n").toString();
  }
}