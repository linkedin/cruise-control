/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.config.TopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.describeLogDirs;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;

@JsonResponseClass
public class KafkaClusterState extends AbstractCruiseControlResponse {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaClusterState.class);
  @JsonResponseField
  protected static final String KAFKA_BROKER_STATE = "KafkaBrokerState";
  @JsonResponseField
  protected static final String KAFKA_PARTITION_STATE = "KafkaPartitionState";
  protected static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";
  protected static final int DEFAULT_MIN_INSYNC_REPLICAS = 1;
  protected final Map<String, Properties> _allTopicConfigs;
  protected final Properties _clusterConfigs;
  protected final Map<String, Object> _adminClientConfigs;
  protected Cluster _kafkaCluster;

  public KafkaClusterState(Cluster kafkaCluster,
                           TopicConfigProvider topicConfigProvider,
                           Map<String, Object> adminClientConfigs,
                           KafkaCruiseControlConfig config) {
    super(config);
    _kafkaCluster = kafkaCluster;
    _allTopicConfigs = topicConfigProvider.allTopicConfigs();
    _clusterConfigs = topicConfigProvider.clusterConfigs();
    _adminClientConfigs = adminClientConfigs;
  }

  protected String getJSONString(CruiseControlParameters parameters) {
    Gson gson = new Gson();
    Map<String, Object> jsonStructure;
    KafkaClusterStateParameters kafkaClusterStateParams = (KafkaClusterStateParameters) parameters;
    boolean isVerbose = kafkaClusterStateParams.isVerbose();
    Pattern topic = kafkaClusterStateParams.topic();
    try {
      jsonStructure = getJsonStructure(isVerbose, topic);
      jsonStructure.put(VERSION, JSON_VERSION);
    }  catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to populate broker logDir state.", e);
    }
    return gson.toJson(jsonStructure);
  }

  /**
   * Get the effective config value of {@link #MIN_INSYNC_REPLICAS} for the given topic.
   *
   * @param topic Topic for which the {@link #MIN_INSYNC_REPLICAS} is queried
   */
  protected int minInsyncReplicas(String topic) {
    Properties topicLevelConfig = _allTopicConfigs.get(topic);
    if (topicLevelConfig != null && topicLevelConfig.get(MIN_INSYNC_REPLICAS) != null) {
      return Integer.parseInt(topicLevelConfig.getProperty(MIN_INSYNC_REPLICAS));
    } else if (_clusterConfigs != null && _clusterConfigs.get(MIN_INSYNC_REPLICAS) != null) {
      return Integer.parseInt(_clusterConfigs.getProperty(MIN_INSYNC_REPLICAS));
    } else {
      return DEFAULT_MIN_INSYNC_REPLICAS;
    }
  }

  /**
   * Return an object that can be further used to encode into JSON.
   *
   * @param verbose True if verbose, false otherwise.
   * @param topic Regex of topic to filter partition states by, is null if no filter is to be applied
   * @return An object that can be further used to encode into JSON.
   */
  protected Map<String, Object> getJsonStructure(boolean verbose, Pattern topic)
      throws ExecutionException, InterruptedException {
    Map<String, Object> cruiseControlState = new HashMap<>();
    cruiseControlState.put(KAFKA_BROKER_STATE, new ClusterBrokerState().getJsonStructure());
    cruiseControlState.put(KAFKA_PARTITION_STATE, new ClusterPartitionState(verbose, topic).getJsonStructure());
    return cruiseControlState;
  }

  protected String getPlaintext(CruiseControlParameters parameters) {
    KafkaClusterStateParameters kafkaClusterStateParams = (KafkaClusterStateParameters) parameters;
    boolean isVerbose = kafkaClusterStateParams.isVerbose();
    Pattern topic = kafkaClusterStateParams.topic();
    StringBuilder sb = new StringBuilder();
    try {
      // Broker summary.
      new ClusterBrokerState().writeBrokerSummary(sb);
      // Partition summary.
      new ClusterPartitionState(isVerbose, topic).writePartitionSummary(sb, isVerbose);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to populate broker logDir state.", e);
    }
    return sb.toString();
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJSONString(parameters) : getPlaintext(parameters);
    // Discard irrelevant response.
    _kafkaCluster = null;
  }

  @JsonResponseClass
  protected class ClusterBrokerState {
    @JsonResponseField
    protected static final String LEADER_COUNT = "LeaderCountByBrokerId";
    @JsonResponseField
    protected static final String OUT_OF_SYNC_COUNT = "OutOfSyncCountByBrokerId";
    @JsonResponseField
    protected static final String REPLICA_COUNT = "ReplicaCountByBrokerId";
    @JsonResponseField
    protected static final String OFFLINE_REPLICA_COUNT = "OfflineReplicaCountByBrokerId";
    @JsonResponseField
    protected static final String IS_CONTROLLER = "IsController";
    @JsonResponseField
    protected static final String ONLINE_LOGDIRS = "OnlineLogDirsByBrokerId";
    @JsonResponseField
    protected static final String OFFLINE_LOGDIRS = "OfflineLogDirsByBrokerId";
    Map<Integer, Integer> _leaderCountByBrokerId;
    Map<Integer, Integer> _outOfSyncCountByBrokerId;
    Map<Integer, Integer> _replicaCountByBrokerId;
    Map<Integer, Integer> _offlineReplicaCountByBrokerId;
    Map<Integer, Boolean> _isControllerByBrokerId;
    Map<Integer, Set<String>> _onlineLogDirsByBrokerId;
    Map<Integer, Set<String>> _offlineLogDirsByBrokerId;

    ClusterBrokerState() throws ExecutionException, InterruptedException {
      _leaderCountByBrokerId = new TreeMap<>();
      _outOfSyncCountByBrokerId = new TreeMap<>();
      _replicaCountByBrokerId = new TreeMap<>();
      _offlineReplicaCountByBrokerId = new TreeMap<>();
      _isControllerByBrokerId = new TreeMap<>();
      // Gather the broker state.
      populateKafkaBrokerState(_leaderCountByBrokerId,
                               _outOfSyncCountByBrokerId,
                               _replicaCountByBrokerId,
                               _offlineReplicaCountByBrokerId,
                               _isControllerByBrokerId);

      _onlineLogDirsByBrokerId = new TreeMap<>();
      _offlineLogDirsByBrokerId = new TreeMap<>();
      // Broker LogDirs Summary
      populateKafkaBrokerLogDirState(_onlineLogDirsByBrokerId, _offlineLogDirsByBrokerId, _replicaCountByBrokerId.keySet());
    }

    protected Map<String, Object> getJsonStructure() {
      Map<String, Object> jsonMap = new HashMap<>(7);
      jsonMap.put(LEADER_COUNT, _leaderCountByBrokerId);
      jsonMap.put(OUT_OF_SYNC_COUNT, _outOfSyncCountByBrokerId);
      jsonMap.put(REPLICA_COUNT, _replicaCountByBrokerId);
      jsonMap.put(OFFLINE_REPLICA_COUNT, _offlineReplicaCountByBrokerId);
      jsonMap.put(IS_CONTROLLER, _isControllerByBrokerId);
      jsonMap.put(ONLINE_LOGDIRS, _onlineLogDirsByBrokerId);
      jsonMap.put(OFFLINE_LOGDIRS, _offlineLogDirsByBrokerId);
      return jsonMap;
    }

    /**
     * Gather the Kafka broker state within the given under leader, out-of-sync, and replica counts.
     *
     * @param leaderCountByBrokerId Leader count by broker id.
     * @param outOfSyncCountByBrokerId Out of sync replica count by broker id.
     * @param replicaCountByBrokerId Replica count by broker id.
     * @param offlineReplicaCountByBrokerId Offline replica count by broker id.
     * @param isControllerByBrokerId Controller information by broker id.
     */
    protected void populateKafkaBrokerState(Map<Integer, Integer> leaderCountByBrokerId,
                                            Map<Integer, Integer> outOfSyncCountByBrokerId,
                                            Map<Integer, Integer> replicaCountByBrokerId,
                                            Map<Integer, Integer> offlineReplicaCountByBrokerId,
                                            Map<Integer, Boolean> isControllerByBrokerId) {
      // Part-1: Gather the states of brokers with replicas.
      for (String topic : _kafkaCluster.topics()) {
        for (PartitionInfo partitionInfo : _kafkaCluster.partitionsForTopic(topic)) {
          if (partitionInfo.leader() == null) {
            continue;
          }
          leaderCountByBrokerId.merge(partitionInfo.leader().id(), 1, Integer::sum);

          Set<Integer> replicas = Arrays.stream(partitionInfo.replicas()).map(Node::id).collect(Collectors.toSet());
          Set<Integer> inSyncReplicas = Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::id).collect(Collectors.toSet());
          Set<Integer> outOfSyncReplicas = new HashSet<>(replicas);
          outOfSyncReplicas.removeAll(inSyncReplicas);
          Set<Integer> offlineReplicas = Arrays.stream(partitionInfo.offlineReplicas()).map(Node::id).collect(Collectors.toSet());

          outOfSyncReplicas.forEach(brokerId -> outOfSyncCountByBrokerId.merge(brokerId, 1, Integer::sum));
          offlineReplicas.forEach(brokerId -> offlineReplicaCountByBrokerId.merge(brokerId, 1, Integer::sum));
          replicas.forEach(brokerId -> replicaCountByBrokerId.merge(brokerId, 1, Integer::sum));
        }
      }
      // Part-2: Gather the states of brokers without replicas.
      for (Node node : _kafkaCluster.nodes()) {
        int nodeId = node.id();
        if (replicaCountByBrokerId.get(nodeId) == null) {
          offlineReplicaCountByBrokerId.put(nodeId, 0);
          replicaCountByBrokerId.put(nodeId, 0);
          outOfSyncCountByBrokerId.put(nodeId, 0);
          leaderCountByBrokerId.put(nodeId, 0);
        }
      }
      // Part-3: Gather controller information.
      replicaCountByBrokerId.keySet().forEach(brokerId -> isControllerByBrokerId.put(brokerId, false));
      Node controller = _kafkaCluster.controller();
      if (controller != null) {
        isControllerByBrokerId.put(controller.id(), true);
      }
    }

    protected void populateKafkaBrokerLogDirState(Map<Integer, Set<String>> onlineLogDirsByBrokerId,
                                                  Map<Integer, Set<String>> offlineLogDirsByBrokerId,
                                                  Set<Integer> brokers)
        throws ExecutionException, InterruptedException {
      // If the broker does not show up in latest metadata, the broker is dead.
      Set<Integer> aliveBrokers = new HashSet<>(brokers.size());
      _kafkaCluster.nodes().forEach(node -> aliveBrokers.add(node.id()));
      for (Integer broker: brokers) {
        if (!aliveBrokers.contains(broker)) {
          onlineLogDirsByBrokerId.put(broker, Collections.singleton("broker_dead"));
          offlineLogDirsByBrokerId.put(broker, Collections.singleton("broker_dead"));
        }
      }

      Map<Integer, KafkaFuture<Map<String, LogDirInfo>>> logDirsByBrokerId = describeLogDirs(aliveBrokers, _adminClientConfigs).values();
      for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirInfo>>> entry : logDirsByBrokerId.entrySet()) {
        onlineLogDirsByBrokerId.put(entry.getKey(), new HashSet<>());
        offlineLogDirsByBrokerId.put(entry.getKey(), new HashSet<>());

        try {
          entry.getValue().get(_config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS).forEach((key, value) -> {
            if (value.error == Errors.NONE) {
              onlineLogDirsByBrokerId.get(entry.getKey()).add(key);
            } else {
              offlineLogDirsByBrokerId.get(entry.getKey()).add(key);
            }
          });
        } catch (TimeoutException te) {
          LOG.error("Getting log dir information for broker {} timed out.", entry.getKey());
          onlineLogDirsByBrokerId.get(entry.getKey()).add("timed_out");
          offlineLogDirsByBrokerId.get(entry.getKey()).add("timed_out");
        }
      }
    }

    protected void writeBrokerSummary(StringBuilder sb) {
      String initMessage = "Brokers:";
      sb.append(String.format("%s%n%20s%20s%20s%20s%20s%20s%n", initMessage, "BROKER", "LEADER(S)", "REPLICAS", "OUT-OF-SYNC",
                              "OFFLINE", "IS_CONTROLLER"));

      for (Integer brokerId : _replicaCountByBrokerId.keySet()) {
        sb.append(String.format("%20d%20d%20d%20d%20d%20s%n",
                                brokerId,
                                _leaderCountByBrokerId.getOrDefault(brokerId, 0),
                                _replicaCountByBrokerId.getOrDefault(brokerId, 0),
                                _outOfSyncCountByBrokerId.getOrDefault(brokerId, 0),
                                _offlineReplicaCountByBrokerId.getOrDefault(brokerId, 0),
                                _isControllerByBrokerId.get(brokerId)));
      }
      // Broker LogDirs Summary
      writeKafkaBrokerLogDirState(sb, _replicaCountByBrokerId.keySet());
    }

    protected void writeKafkaBrokerLogDirState(StringBuilder sb, Set<Integer> brokers) {
      int onlineLogDirsNameLength = _onlineLogDirsByBrokerId.values().stream().mapToInt(dir -> dir.toString().length())
                                                            .max().orElse(20) + 14;
      int offlineLogDirsNameLength = _offlineLogDirsByBrokerId.values().stream().mapToInt(dir -> dir.toString().length())
                                                              .max().orElse(20) + 15;

      String initMessage = "LogDirs of brokers with replicas:";
      sb.append(String.format("%n%s%n%20s%" + onlineLogDirsNameLength + "s%" + offlineLogDirsNameLength + "s%n",
                              initMessage, "BROKER", "ONLINE-LOGDIRS", "OFFLINE-LOGDIRS"));

      for (int brokerId : brokers) {
        sb.append(String.format("%20d%" + onlineLogDirsNameLength + "s%" + offlineLogDirsNameLength + "s%n",
                                brokerId,
                                _onlineLogDirsByBrokerId.get(brokerId).toString(),
                                _offlineLogDirsByBrokerId.get(brokerId).toString()));
      }
    }
  }

  @JsonResponseClass
  protected class ClusterPartitionState {
    @JsonResponseField
    protected static final String OFFLINE = "offline";
    @JsonResponseField
    protected static final String WITH_OFFLINE_REPLICAS = "with-offline-replicas";
    @JsonResponseField
    protected static final String URP = "urp";
    @JsonResponseField
    protected static final String UNDER_MIN_ISR = "under-min-isr";
    @JsonResponseField(required = false)
    protected static final String OTHER = "other";

    Set<PartitionInfo> _underReplicatedPartitions;
    Set<PartitionInfo> _offlinePartitions;
    Set<PartitionInfo> _otherPartitions;
    Set<PartitionInfo> _partitionsWithOfflineReplicas;
    Set<PartitionInfo> _underMinIsrPartitions;

    ClusterPartitionState(boolean verbose, Pattern topicPattern) {
      Comparator<PartitionInfo> comparator = Comparator.comparing(PartitionInfo::topic).thenComparingInt(PartitionInfo::partition);
      _underReplicatedPartitions = new TreeSet<>(comparator);
      _offlinePartitions = new TreeSet<>(comparator);
      _otherPartitions = new TreeSet<>(comparator);
      _partitionsWithOfflineReplicas = new TreeSet<>(comparator);
      _underMinIsrPartitions = new TreeSet<>(comparator);
      // Gather the partition state.
      populateKafkaPartitionState(_underReplicatedPartitions, _offlinePartitions, _otherPartitions,
                                  _partitionsWithOfflineReplicas, _underMinIsrPartitions, verbose, topicPattern);
    }

    protected Map<String, Object> getJsonStructure() {
      // Write the partition state.
      Map<String, Object> jsonMap = new HashMap<>();
      jsonMap.put(OFFLINE, getJsonPartitions(_offlinePartitions));
      jsonMap.put(WITH_OFFLINE_REPLICAS, getJsonPartitions(_partitionsWithOfflineReplicas));
      jsonMap.put(URP, getJsonPartitions(_underReplicatedPartitions));
      jsonMap.put(UNDER_MIN_ISR, getJsonPartitions(_underMinIsrPartitions));
      if (!_otherPartitions.isEmpty()) {
        jsonMap.put(OTHER, getJsonPartitions(_otherPartitions));
      }
      return jsonMap;
    }

    /**
     * Gather the Kafka partition state within the given under replicated, offline, under minIsr,
     * and other partitions (if verbose).
     *
     * @param underReplicatedPartitions state of under replicated partitions.
     * @param offlinePartitions state of offline partitions.
     * @param otherPartitions state of partitions other than offline or urp.
     * @param partitionsWithOfflineReplicas state of partitions with offline replicas.
     * @param underMinIsrPartitions state of under min isr partitions.
     * @param verbose true if requested to gather state of partitions other than offline or urp.
     * @param topicPattern regex of topic to filter partition states by, is null if no filter is to be applied
     */
    protected void populateKafkaPartitionState(Set<PartitionInfo> underReplicatedPartitions,
                                               Set<PartitionInfo> offlinePartitions,
                                               Set<PartitionInfo> otherPartitions,
                                               Set<PartitionInfo> partitionsWithOfflineReplicas,
                                               Set<PartitionInfo> underMinIsrPartitions,
        boolean verbose,
        Pattern topicPattern) {
      for (String topic : _kafkaCluster.topics()) {
        if (topicPattern == null || topicPattern.matcher(topic).matches()) {
          int minInsyncReplicas = minInsyncReplicas(topic);
          for (PartitionInfo partitionInfo : _kafkaCluster.partitionsForTopic(topic)) {
            int numInsyncReplicas = partitionInfo.inSyncReplicas().length;
            boolean isURP = numInsyncReplicas != partitionInfo.replicas().length;
            if (numInsyncReplicas < minInsyncReplicas) {
              underMinIsrPartitions.add(partitionInfo);
            }
            if (isURP || verbose) {
              boolean hasOfflineReplica = partitionInfo.offlineReplicas().length != 0;
              if (hasOfflineReplica) {
                partitionsWithOfflineReplicas.add(partitionInfo);
              }
              boolean isOffline = partitionInfo.inSyncReplicas().length == 0;
              if (isOffline) {
                offlinePartitions.add(partitionInfo);
              } else if (isURP) {
                underReplicatedPartitions.add(partitionInfo);
              } else {
                // verbose -- other
                otherPartitions.add(partitionInfo);
              }
            }
          }
        }
      }
    }

    protected List<Object> getJsonPartitions(Set<PartitionInfo> partitions) {
      List<Object> partitionList = new ArrayList<>();
      for (PartitionInfo partitionInfo : partitions) {
        partitionList.add(new PartitionState(partitionInfo).getJsonStructure());
      }

      return partitionList;
    }

    protected void writePartitionSummary(StringBuilder sb, boolean verbose) {
      int topicNameLength = _kafkaCluster.topics().stream().mapToInt(String::length).max().orElse(20) + 5;

      String initMessage = verbose ? "All Partitions in the Cluster (verbose):"
                                   : "Under Replicated, Offline, and Under MinIsr Partitions:";
      sb.append(String.format("%n%s%n%" + topicNameLength + "s%10s%10s%30s%30s%25s%25s%n", initMessage, "TOPIC",
                              "PARTITION", "LEADER", "REPLICAS", "IN-SYNC", "OUT-OF-SYNC", "OFFLINE"));

      // Write the cluster state.
      sb.append(String.format("Offline Partitions:%n"));
      writeKafkaPartitionState(sb, _offlinePartitions, topicNameLength);

      sb.append(String.format("Partitions with Offline Replicas:%n"));
      writeKafkaPartitionState(sb, _partitionsWithOfflineReplicas, topicNameLength);

      sb.append(String.format("Under Replicated Partitions:%n"));
      writeKafkaPartitionState(sb, _underReplicatedPartitions, topicNameLength);

      sb.append(String.format("Under MinIsr Partitions:%n"));
      writeKafkaPartitionState(sb, _underMinIsrPartitions, topicNameLength);

      if (verbose) {
        sb.append(String.format("Other Partitions:%n"));
        writeKafkaPartitionState(sb, _otherPartitions, topicNameLength);
      }
    }

    protected void writeKafkaPartitionState(StringBuilder sb, Set<PartitionInfo> partitions, int topicNameLength) {
      for (PartitionInfo partitionInfo : partitions) {
        sb.append(new PartitionState(partitionInfo).writeKafkaPartitionState(topicNameLength));
      }
    }
  }


  @JsonResponseClass
  protected static class PartitionState {
    @JsonResponseField
    protected static final String TOPIC = "topic";
    @JsonResponseField
    protected static final String PARTITION = "partition";
    @JsonResponseField
    protected static final String LEADER = "leader";
    @JsonResponseField
    protected static final String REPLICAS = "replicas";
    @JsonResponseField
    protected static final String IN_SYNC = "in-sync";
    @JsonResponseField
    protected static final String OUT_OF_SYNC = "out-of-sync";
    @JsonResponseField
    protected static final String OFFLINE = "offline";
    String _topic;
    int _partition;
    int _leader;
    List<Integer> _replicas;
    List<Integer> _inSyncReplicas;
    Set<Integer> _outOfSyncReplicas;
    Set<Integer> _offlineReplicas;

    PartitionState(PartitionInfo partitionInfo) {
      _topic = partitionInfo.topic();
      _partition = partitionInfo.partition();
      _leader = partitionInfo.leader() == null ? -1 : partitionInfo.leader().id();
      _replicas = Arrays.stream(partitionInfo.replicas()).map(Node::id).collect(Collectors.toList());
      _inSyncReplicas = Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::id).collect(Collectors.toList());
      _outOfSyncReplicas = new HashSet<>(_replicas);
      _outOfSyncReplicas.removeAll(_inSyncReplicas);
      _offlineReplicas = Arrays.stream(partitionInfo.offlineReplicas()).map(Node::id).collect(Collectors.toSet());
    }

    protected Map<String, Object> getJsonStructure() {
      Map<String, Object> recordMap = new HashMap<>(7);
      recordMap.put(TOPIC, _topic);
      recordMap.put(PARTITION, _partition);
      recordMap.put(LEADER, _leader);
      recordMap.put(REPLICAS, _replicas);
      recordMap.put(IN_SYNC, _inSyncReplicas);
      recordMap.put(OUT_OF_SYNC, _outOfSyncReplicas);
      recordMap.put(OFFLINE, _offlineReplicas);
      return recordMap;
    }

    protected String writeKafkaPartitionState(int topicNameLength) {
      return String.format("%" + topicNameLength + "s%10s%10s%30s%30s%25s%25s%n",
                           _topic, _partition,
                           _replicas,
                           _inSyncReplicas,
                           _outOfSyncReplicas,
                           _offlineReplicas);
    }
  }
}