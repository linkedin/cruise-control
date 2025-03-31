/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.netflix.kaas;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicInfo {
    private String topicName;
    private int clusterId;
    private String clusterApp;
    private String clusterStack;
    private String account;
    private String region;
    private int numPartitions;
    private int numReplicas;
    private boolean compacted;
    private String state;
    private List<TopicPartitionInfo> partitions;
    private int leadershipCount;
    private int underReplicatedCount;
    private boolean hasOfflinePartitions;
    private double health;
}
