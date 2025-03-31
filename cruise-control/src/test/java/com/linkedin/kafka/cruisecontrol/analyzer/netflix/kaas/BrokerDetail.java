package com.linkedin.kafka.cruisecontrol.analyzer.netflix.kaas;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BrokerDetail {
    private int topicCount;
    private int partitionCount;
    private int leadershipCount;
    private double health;
    private boolean controller;
    private boolean hasOfflinePartitions;
    private List<TopicInfo> topicList;
}
