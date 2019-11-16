/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.Extrapolation;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.SampleExtrapolation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.model.Disk.State.DEAD;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.CPU_USAGE;
import static java.lang.Thread.sleep;


/**
 * A util class for Monitor.
 */
public class MonitorUtils {
  // A utility variable for conversion of unit interval to percentage -- i.e. [0, 1.0] -> [0, 100.0].
  public static final double UNIT_INTERVAL_TO_PERCENTAGE = 100.0;
  private static final Logger LOG = LoggerFactory.getLogger(MonitorUtils.class);

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
  private static AggregatedMetricValues toFollowerMetricValues(AggregatedMetricValues aggregatedMetricValues) {
    AggregatedMetricValues followerLoad = new AggregatedMetricValues();
    for (short metricId : aggregatedMetricValues.metricIds()) {
      String metricGroup = KafkaMetricDef.commonMetricDef().metricInfo(metricId).group();
      if (!Resource.CPU.name().equals(metricGroup) && !Resource.NW_OUT.name().equals(metricGroup)) {
        followerLoad.add(metricId, aggregatedMetricValues.valuesFor(metricId));
      }
    }
    MetricValues followerCpu = new MetricValues(aggregatedMetricValues.length());
    MetricValues leaderBytesInRate =
        aggregatedMetricValues.valuesForGroup(Resource.NW_IN.name(), KafkaMetricDef.commonMetricDef(), false);
    MetricValues leaderBytesOutRate =
        aggregatedMetricValues.valuesForGroup(Resource.NW_OUT.name(), KafkaMetricDef.commonMetricDef(), false);
    MetricValues leaderCpuUtilization = aggregatedMetricValues.valuesFor(KafkaMetricDef.commonMetricDefId(CPU_USAGE));
    for (int i = 0; i < aggregatedMetricValues.length(); i++) {
      double followerCpuUtil = ModelUtils.getFollowerCpuUtilFromLeaderLoad(leaderBytesInRate.get(i),
                                                                           leaderBytesOutRate.get(i),
                                                                           leaderCpuUtilization.get(i));
      followerCpu.set(i, followerCpuUtil);
    }
    for (short nwOutMetricId : KafkaMetricDef.resourceToMetricIds(Resource.NW_OUT)) {
      followerLoad.add(nwOutMetricId, new MetricValues(aggregatedMetricValues.length()));
    }
    followerLoad.add(KafkaMetricDef.commonMetricDefId(CPU_USAGE), followerCpu);
    return followerLoad;
  }

  /**
   * @return True if the metadata has changed, false otherwise.
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

  /**
   * Combine load requirement options.
   *
   * @param goals Goals for which the load requirement options will be combined.
   * @return Combined load requirement options.
   */
  public static ModelCompletenessRequirements combineLoadRequirementOptions(Collection<Goal> goals) {
    ModelCompletenessRequirements requirements = null;
    for (Goal goal : goals) {
      requirements = goal.clusterModelCompletenessRequirements().stronger(requirements);
    }
    return requirements;
  }

  /**
   * Get the total number of partitions in the cluster.
   *
   * @param cluster Kafka cluster.
   * @return The total number of partitions in the cluster.
   */
  public static int totalNumPartitions(Cluster cluster) {
    int totalNumPartitions = 0;
    for (String topic : cluster.topics()) {
      totalNumPartitions += cluster.partitionCountForTopic(topic);
    }
    return totalNumPartitions;
  }

  /**
   * Check whether the topic has partitions undergoing partition reassignment and wait for the reassignments to finish.
   *
   * @param kafkaZkClient the KafkaZkClient class used to check ongoing partition reassignments.
   * @return Whether there are no ongoing partition reassignments.
   */
  public static boolean ensureTopicNotUnderPartitionReassignment(KafkaZkClient kafkaZkClient, String topic) {
    int attempt = 0;
    while (JavaConversions.asJavaCollection(kafkaZkClient.getPartitionReassignment().keys()).stream()
                          .anyMatch(tp -> tp.topic().equals(topic))) {
      try {
        sleep(1000 << attempt);
      } catch (InterruptedException e) {
        // Let it go.
      }
      if (++attempt == 10) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check whether there are ongoing partition reassignments and wait for the reassignments to finish.
   *
   * @param kafkaZkClient the KafkaZkClient class used to check ongoing partition reassignments.
   * @return Whether there are no ongoing partition reassignments.
   */
  public static boolean ensureNoPartitionUnderPartitionReassignment(KafkaZkClient kafkaZkClient) {
    int attempt = 0;
    while (kafkaZkClient.getPartitionReassignment().size() > 0) {
      try {
        sleep(1000 << attempt);
      } catch (InterruptedException e) {
        // Let it go.
      }
      if (++attempt == 10) {
        return false;
      }
    }
    return true;
  }

  /**
   * Convert replica's cpu usage metric from absolute value to percentage value since the cpu capacity reported by
   * {@link BrokerCapacityConfigResolver} is percentage value.
   *
   * @param aggregatedMetricValues the {@link AggregatedMetricValues} for the replica.
   */
  private static void adjustCpuUsage(AggregatedMetricValues aggregatedMetricValues) {
    short cpuUsageId = KafkaMetricDef.commonMetricDefId(KafkaMetricDef.CPU_USAGE);
    MetricValues cpuUsage = aggregatedMetricValues.valuesFor(cpuUsageId);
    for (int i = 0; i < cpuUsage.length(); i++) {
      cpuUsage.set(i, cpuUsage.get(i) * UNIT_INTERVAL_TO_PERCENTAGE);
    }
  }

  /**
   * Get the {@link AggregatedMetricValues} based on the replica role (leader/follower) and the replication factor.
   *
   * @param valuesAndExtrapolations the values and extrapolations of the leader replica.
   * @param partitionInfo the partition info.
   * @param isLeader whether the value is created for leader replica or follower replica.
   * @param needToAdjustCpuUsage whether need to cast cpu usage metric for replica from absolute value to percentage.
   * @return The {@link AggregatedMetricValues} to use for the given replica.
   */
  private static AggregatedMetricValues getAggregatedMetricValues(ValuesAndExtrapolations valuesAndExtrapolations,
                                                                  PartitionInfo partitionInfo,
                                                                  boolean isLeader,
                                                                  boolean needToAdjustCpuUsage) {
    AggregatedMetricValues aggregatedMetricValues = valuesAndExtrapolations.metricValues();
    if (needToAdjustCpuUsage) {
      adjustCpuUsage(aggregatedMetricValues);
    }

    return isLeader ? fillInReplicationBytesOut(aggregatedMetricValues, partitionInfo)
                    : toFollowerMetricValues(aggregatedMetricValues);
  }

  /**
   * When the replica is a leader replica, we need to fill in the replication bytes out if it has not been filled in
   * yet. This is because currently Kafka does not report this metric. We simply use the leader bytes in rate multiplied
   * by the number of followers as the replication bytes out rate. The assumption is that all the followers will
   * eventually keep up with the leader.
   *
   * We only fill in the replication bytes out rate when creating the cluster model because the replication factor
   * may have changed since the time the PartitionMetricSample was created.
   *
   * @param aggregatedMetricValues the {@link AggregatedMetricValues} for the leader replica.
   * @param info the partition info for the partition.
   * @return The {@link AggregatedMetricValues} with the replication bytes out rate filled in.
   */
  private static AggregatedMetricValues fillInReplicationBytesOut(AggregatedMetricValues aggregatedMetricValues,
                                                                  PartitionInfo info) {
    int numFollowers = info.replicas().length - 1;
    short leaderBytesInRateId = KafkaMetricDef.commonMetricDefId(KafkaMetricDef.LEADER_BYTES_IN);
    short replicationBytesOutRateId = KafkaMetricDef.commonMetricDefId(KafkaMetricDef.REPLICATION_BYTES_OUT_RATE);

    MetricValues leaderBytesInRate = aggregatedMetricValues.valuesFor(leaderBytesInRateId);
    MetricValues replicationBytesOutRate = aggregatedMetricValues.valuesFor(replicationBytesOutRateId);
    // If the replication bytes out rate is already reported, update it. Otherwise add a new MetricValues.
    if (replicationBytesOutRate == null) {
      replicationBytesOutRate = new MetricValues(leaderBytesInRate.length());
      aggregatedMetricValues.add(replicationBytesOutRateId, replicationBytesOutRate);
    }
    for (int i = 0; i < leaderBytesInRate.length(); i++) {
      replicationBytesOutRate.set(i, leaderBytesInRate.get(i) * numFollowers);
    }

    return aggregatedMetricValues;
  }

  /**
   * @param node The node whose rack is requested.
   * @return Rack of the given node if the corresponding value is not null and not empty, the host of the node otherwise.
   */
  public static String getRackHandleNull(Node node) {
    return node.rack() == null || node.rack().isEmpty() ? node.host() : node.rack();
  }

  /**
   * @param cluster Kafka cluster.
   * @return All the brokers in the cluster that host at least one replica.
   */
  static Set<Integer> brokersWithReplicas(Cluster cluster) {
    Set<Integer> allBrokers = new HashSet<>();
    for (String topic : cluster.topics()) {
      for (PartitionInfo partition : cluster.partitionsForTopic(topic)) {
        Arrays.stream(partition.replicas()).map(Node::id).forEach(allBrokers::add);
      }
    }
    return allBrokers;
  }

  /**
   * @param cluster Kafka cluster.
   * @return All the dead brokers in the cluster that host at least one replica.
   */
  static Set<Integer> deadBrokersWithReplicas(Cluster cluster) {
    Set<Integer> brokersWithReplicas = brokersWithReplicas(cluster);
    cluster.nodes().forEach(node -> brokersWithReplicas.remove(node.id()));
    return brokersWithReplicas;
  }

  /**
   * @param cluster Kafka cluster.
   * @return All the brokers that host at least one offline replica.
   */
  static Set<Integer> brokersWithOfflineReplicas(Cluster cluster) {
    Set<Integer> brokersWithOfflineReplicas = new HashSet<>();
    for (String topic : cluster.topics()) {
      for (PartitionInfo partition : cluster.partitionsForTopic(topic)) {
        if (partition.leader() != null) {
          brokersWithOfflineReplicas.addAll(Arrays.stream(partition.offlineReplicas()).map(Node::id).collect(Collectors.toSet()));
        }
      }
    }
    return brokersWithOfflineReplicas;
  }

  /**
   * Set the state of bad brokers in clusterModel based on the given Kafka cluster state:
   * <ul>
   *   <li>Get the dead brokers and mark them as dead.</li>
   *   <li>Get the alive brokers with bad disks and mark them accordingly.</li>
   * </ul>
   *
   * @param clusterModel The cluster model to set the broker states.
   * @param cluster Kafka cluster.
   */
  static void setBadBrokerState(ClusterModel clusterModel, Cluster cluster) {
    MonitorUtils.deadBrokersWithReplicas(cluster).forEach(brokerId -> clusterModel.setBrokerState(brokerId, Broker.State.DEAD));
    for (Integer brokerId : MonitorUtils.brokersWithOfflineReplicas(cluster)) {
      if (clusterModel.broker(brokerId).isAlive()) {
        clusterModel.setBrokerState(brokerId, Broker.State.BAD_DISKS);
      }
    }
  }

  /**
   * @param valuesAndExtrapolations The aggregated metric values for windows and the extrapolation information by partitions.
   * @return Sample extrapolations by partitions.
   */
  static Map<TopicPartition, List<SampleExtrapolation>> partitionSampleExtrapolations(Map<PartitionEntity, ValuesAndExtrapolations> valuesAndExtrapolations) {
    Map<TopicPartition, List<SampleExtrapolation>> sampleExtrapolations = new HashMap<>();
    for (Map.Entry<PartitionEntity, ValuesAndExtrapolations> entry : valuesAndExtrapolations.entrySet()) {
      TopicPartition tp = entry.getKey().tp();
      Map<Integer, Extrapolation> extrapolations = entry.getValue().extrapolations();
      if (!extrapolations.isEmpty()) {
        List<SampleExtrapolation> extrapolationForPartition = sampleExtrapolations.computeIfAbsent(tp, p -> new ArrayList<>());
        extrapolations.forEach((t, extrapolation) -> extrapolationForPartition.add(new SampleExtrapolation(t, extrapolation)));
      }
    }

    return sampleExtrapolations;
  }

  /**
   * Get replica placement information, i.e. each replica resides on which logdir of the broker.
   *
   * @param clusterModel The cluster model to populate replica placement information.
   * @param cluster Kafka cluster.
   * @param adminClient Admin client to send request to kafka cluster
   * @param config Kafka Cruise Control relate config object
   * @return A map from topic partition to replica placement information.
   *
   */
  static Map<TopicPartition, Map<Integer, String>> getReplicaPlacementInfo(ClusterModel clusterModel,
                                                                           Cluster cluster,
                                                                           AdminClient adminClient,
                                                                           KafkaCruiseControlConfig config) {
    Map<TopicPartition, Map<Integer, String>> replicaPlacementInfo = new HashMap<>();
    Map<Integer, KafkaFuture<Map<String, DescribeLogDirsResponse.LogDirInfo>>> logDirsByBrokerId =
        adminClient.describeLogDirs(cluster.nodes().stream().mapToInt(Node::id).boxed().collect(Collectors.toList())).values();
    for (Map.Entry<Integer, KafkaFuture<Map<String, DescribeLogDirsResponse.LogDirInfo>>> entry : logDirsByBrokerId.entrySet()) {
      Integer brokerId = entry.getKey();
      try {
        entry.getValue().get(config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS).forEach((logdir, info) -> {
          if (info.error == Errors.NONE) {
            for (Map.Entry<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> e : info.replicaInfos.entrySet()) {
              if (!e.getValue().isFuture) {
                replicaPlacementInfo.putIfAbsent(e.getKey(), new HashMap<>());
                replicaPlacementInfo.get(e.getKey()).put(brokerId, logdir);
              } else {
                LOG.trace("Topic partition {}'s replica is moving to {} on broker {}.", e.getKey(), logdir, brokerId);
              }
            }
          } else {
            clusterModel.broker(brokerId).disk(logdir).setState(DEAD);
          }
        });
      } catch (TimeoutException te) {
        throw new RuntimeException(String.format("Getting logdir information for broker %d encountered TimeoutException %s.",
                                                 entry.getKey(), te));
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(String.format("Populating logdir information for broker %d encountered Exception %s.",
                                                 entry.getKey(), e));
      }
    }
    return replicaPlacementInfo;
  }

  /**
   * Create replicas of the partition with the given (1) identifier and (2) load information to populate the given cluster model.
   * If partition with the given identifier does not exist in the given cluster, do nothing.
   *
   * @param cluster Kafka cluster.
   * @param clusterModel The cluster model to populate load information.
   * @param tp Topic partition that identifies the partition to populate the load for.
   * @param valuesAndExtrapolations The values and extrapolations of the leader replica.
   * @param replicaPlacementInfo The distribution of replicas over broker logdirs if available, {@code null} otherwise.
   * @param brokerCapacityConfigResolver The resolver for retrieving broker capacities.
   */
  static void populatePartitionLoad(Cluster cluster,
                                    ClusterModel clusterModel,
                                    TopicPartition tp,
                                    ValuesAndExtrapolations valuesAndExtrapolations,
                                    Map<TopicPartition, Map<Integer, String>> replicaPlacementInfo,
                                    BrokerCapacityConfigResolver brokerCapacityConfigResolver) {
    PartitionInfo partitionInfo = cluster.partition(tp);
    // If partition info does not exist, the topic may have been deleted.
    if (partitionInfo != null) {
      boolean needToAdjustCpuUsage = true;
      for (int index = 0; index < partitionInfo.replicas().length; index++) {
        Node replica = partitionInfo.replicas()[index];
        String rack = getRackHandleNull(replica);
        // Note that we assume the capacity resolver can still return the broker capacity even if the broker
        // is dead. We need this to get the host resource capacity.
        BrokerCapacityInfo brokerCapacity =
            brokerCapacityConfigResolver.capacityForBroker(rack, replica.host(), replica.id());
        clusterModel.handleDeadBroker(rack, replica.id(), brokerCapacity);
        boolean isLeader;
        if (partitionInfo.leader() == null) {
          LOG.warn("Detected offline partition {}-{}, skipping", partitionInfo.topic(), partitionInfo.partition());
          continue;
        } else {
          isLeader = replica.id() == partitionInfo.leader().id();
        }
        boolean isOffline = Arrays.stream(partitionInfo.offlineReplicas())
                                  .anyMatch(offlineReplica -> offlineReplica.id() == replica.id());

        String logdir = replicaPlacementInfo == null ? null : replicaPlacementInfo.get(tp).get(replica.id());
        // If the replica's logdir is null, it is either because replica placement information is not populated for the cluster
        // model or this replica is hosted on a dead disk and is not considered for intra-broker replica operations.
        clusterModel.createReplica(rack, replica.id(), tp, index, isLeader, isOffline, logdir, false);
        clusterModel.setReplicaLoad(rack,
                                    replica.id(),
                                    tp,
                                    getAggregatedMetricValues(valuesAndExtrapolations,
                                                              cluster.partition(tp),
                                                              isLeader,
                                                              needToAdjustCpuUsage),
                                    valuesAndExtrapolations.windows());
        needToAdjustCpuUsage = false;
      }
    }
  }
}
