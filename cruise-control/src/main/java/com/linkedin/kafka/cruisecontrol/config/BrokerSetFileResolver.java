/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import com.google.gson.Gson;


/**
 * BrokerSet information store implementation based out of File config broker.set.config.file
 * By default the property is set to value of brokerSets.json
 *
 * Example Broker Set Data File :
 * <pre>
 * {
 *   "brokerSets":[
 *     {
 *       "brokerSetId": "Blue",
 *       "brokerIds": [0, 1, 2]
 *     },
 *     {
 *       "brokerSetId": "Green",
 *       "brokerIds": [3, 4, 5]
 *     }
 *   ]
 * }
 * </pre>
 */
public class BrokerSetFileResolver implements BrokerSetResolver {
  private String _configFile;
  private BrokerSetAssignmentPolicy _defaultBrokerSetAssignmentPolicy;

  @Override
  public void configure(Map<String, ?> configs) {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(configs, false);
    _configFile = config.getString(AnalyzerConfig.BROKER_SET_CONFIG_FILE_CONFIG);
    _defaultBrokerSetAssignmentPolicy =
        config.getConfiguredInstance(AnalyzerConfig.BROKER_SET_ASSIGNMENT_POLICY_CLASS_CONFIG, BrokerSetAssignmentPolicy.class);
  }

  @Override
  public Map<String, Set<Integer>> brokerIdsByBrokerSetId(ClusterModel clusterModel) throws BrokerSetResolutionException {
    Map<String, Set<Integer>> brokerIdsByBrokerSetId;
    try {
      brokerIdsByBrokerSetId = loadBrokerSetData();
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }

    return _defaultBrokerSetAssignmentPolicy.assignBrokerSetsForUnresolvedBrokers(clusterModel, brokerIdsByBrokerSetId);
  }

  private Map<String, Set<Integer>> loadBrokerSetData() throws IOException {
    try (Reader reader = Files.newBufferedReader(Path.of(_configFile), StandardCharsets.UTF_8)) {
      Gson gson = new Gson();
      final BrokerSets brokerSets = gson.fromJson(reader, BrokerSets.class);
      final Set<BrokerSet> brokerSetSet = brokerSets.brokerSets;
      final Map<String, Set<Integer>> brokerIdsByBrokerSetId = new HashMap<>();
      for (BrokerSet brokerSet : brokerSetSet) {
        brokerIdsByBrokerSetId.put(brokerSet.brokerSetId, brokerSet.brokerIds);
      }
      return brokerIdsByBrokerSetId;
    }
  }

  private static class BrokerSets {
    private Set<BrokerSetFileResolver.BrokerSet> brokerSets;
  }

  private static class BrokerSet {
    private final String brokerSetId;
    private final Set<Integer> brokerIds;

    BrokerSet(String brokerSetId, Set<Integer> brokerIds) {
      this.brokerSetId = brokerSetId;
      this.brokerIds = brokerIds;
    }
  }
}
