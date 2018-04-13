/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.CPU_USAGE;


/**
 * A util class for Monitor.
 */
public class MonitorUtils {

  private MonitorUtils() {

  }

  /**
   * Derive follower load from the leader load.
   * <p>
   * If linear regression model is used, the The way we derive the follower metrics is the following:
   * <ul>
   * <li>FOLLOWER_CPU = LEADER_NETWORK_IN * coefficient + residue </li>
   * <li>FOLLOWER_DISK = LEADER_DISK </li>
   * <li>FOLLOWER_BYTES_IN = LEADER_BYTES_IN </li>
   * <li>FOLLOWER_BYTES_OUT = 0 </li>
   * </ul>
   *
   * If linear regression model is not used, CPU utilization of the follower will be fixed to be 0.2;
   *
   * @param aggregatedMetricValues the leader aggregated metric values to convert.
   */
  public static AggregatedMetricValues toFollowerMetricValues(AggregatedMetricValues aggregatedMetricValues) {

    AggregatedMetricValues followerLoad = new AggregatedMetricValues();
    for (int metricId : aggregatedMetricValues.metricIds()) {
      String metricGroup = KafkaMetricDef.commonMetricDef().metricInfo(metricId).group();
      if (!Resource.CPU.name().equals(metricGroup) && !Resource.NW_OUT.name().equals(metricGroup)) {
        followerLoad.add(metricId, aggregatedMetricValues.valuesFor(metricId));
      }
    }
    MetricValues followerCpu = new MetricValues(aggregatedMetricValues.length());
    MetricValues totalNetworkIn =
        aggregatedMetricValues.valuesForGroup(Resource.NW_IN.name(), KafkaMetricDef.commonMetricDef(), false);
    MetricValues totalNetworkOut =
        aggregatedMetricValues.valuesForGroup(Resource.NW_OUT.name(), KafkaMetricDef.commonMetricDef(), false);
    MetricValues totalCpuUsage = aggregatedMetricValues.valuesFor(KafkaMetricDef.commonMetricDefId(CPU_USAGE));
    for (int i = 0; i < aggregatedMetricValues.length(); i++) {
      double followerCpuUtil = ModelUtils.getFollowerCpuUtilFromLeaderLoad(totalNetworkIn.get(i),
                                                                           totalNetworkOut.get(i),
                                                                           totalCpuUsage.get(i));
      followerCpu.set(i, followerCpuUtil);
    }
    for (int nwOutMetricId : KafkaMetricDef.resourceToMetricIds(Resource.NW_OUT)) {
      followerLoad.add(nwOutMetricId, new MetricValues(aggregatedMetricValues.length()));
    }
    followerLoad.add(KafkaMetricDef.commonMetricDefId(CPU_USAGE), followerCpu);
    return followerLoad;
  }

  /**
   * Check whether the metadata has changed.
   */
  public static boolean metadataChanged(Cluster prev, Cluster curr) {
    // Broker has changed.
    Set<Node> prevNodeSet = new HashSet<>(prev.nodes());
    if (prevNodeSet.size() != curr.nodes().size()) {
      return true;
    }
    prevNodeSet.removeAll(curr.nodes());
    if (!prevNodeSet.isEmpty()) {
      return true;
    }
    // Topic has changed
    if (!prev.topics().equals(curr.topics())) {
      return true;
    }

    // partition has changed.
    for (String topic : prev.topics()) {
      if (!prev.partitionCountForTopic(topic).equals(curr.partitionCountForTopic(topic))) {
        return true;
      }
      for (PartitionInfo prevPartInfo : prev.partitionsForTopic(topic)) {
        PartitionInfo currPartInfo = curr.partition(new TopicPartition(prevPartInfo.topic(), prevPartInfo.partition()));
        if (leaderChanged(prevPartInfo, currPartInfo) || replicaListChanged(prevPartInfo, currPartInfo)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean leaderChanged(PartitionInfo prevPartInfo, PartitionInfo currPartInfo) {
    Node prevLeader = prevPartInfo.leader();
    Node currLeader = currPartInfo.leader();
    return !(prevLeader == null && currLeader == null) && !(prevLeader != null && currLeader != null
        && prevLeader.id() == currLeader.id());
  }

  private static boolean replicaListChanged(PartitionInfo prevPartInfo, PartitionInfo currPartInfo) {
    if (prevPartInfo.replicas().length != currPartInfo.replicas().length) {
      return true;
    }
    for (int i = 0; i < prevPartInfo.replicas().length; i++) {
      if (prevPartInfo.replicas()[i].id() != currPartInfo.replicas()[i].id()) {
        return true;
      }
    }
    return false;
  }

  public static ModelCompletenessRequirements combineLoadRequirementOptions(Collection<Goal> goals) {
    ModelCompletenessRequirements requirements = null;
    for (Goal goal : goals) {
      requirements = goal.clusterModelCompletenessRequirements().stronger(requirements);
    }
    return requirements;
  }

  public static int totalNumPartitions(Cluster cluster) {
    int totalNumPartitions = 0;
    for (String topic : cluster.topics()) {
      totalNumPartitions += cluster.partitionCountForTopic(topic);
    }
    return totalNumPartitions;
  }
}
