package com.linkedin.kafka.cruisecontrol.analyzer.netflix.kaas;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class TopicPartitionReplicaInfo {
    private int brokerId;
    @JsonProperty("isLeader")
    private boolean leader;
    private boolean inSync;
}
