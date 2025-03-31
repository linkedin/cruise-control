package com.linkedin.kafka.cruisecontrol.analyzer.netflix.kaas;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import java.util.List;
import java.util.Set;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicPartitionInfo {
    private int id;
    private List<TopicPartitionReplicaInfo> replicas;
    private boolean isOffline;
    private double health;
    private Set<Integer> ownerBrokerSet;
    private Set<Integer> inSyncBrokerSet;
    private Set<Integer> leaderReplicaBrokerSet;
}
