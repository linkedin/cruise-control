package com.linkedin.kafka.cruisecontrol.analyzer.netflix.kaas;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BrokerInfo {
    private int id;
    private String name;
    private String zone;
    private String instanceType;
    private BrokerDetail brokerDetail;
}
