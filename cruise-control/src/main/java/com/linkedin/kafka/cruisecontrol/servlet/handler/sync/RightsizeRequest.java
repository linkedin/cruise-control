/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.common.Utils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.detector.Provisioner;
import com.linkedin.kafka.cruisecontrol.detector.ProvisionerState;
import com.linkedin.kafka.cruisecontrol.detector.RightsizeOptions;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RightsizeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.RightsizeResult;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.RIGHTSIZE_PARAMETER_OBJECT_CONFIG;


public class RightsizeRequest extends AbstractSyncRequest {
  private static final String RECOMMENDER_UP = "Recommender-Under-Provisioned";
  protected KafkaCruiseControl _kafkaCruiseControl;
  protected RightsizeParameters _parameters;

  public RightsizeRequest() {
    super();
  }

  @Override
  protected RightsizeResult handle() {
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    Map<String, Object> overrideConfigs;
    overrideConfigs = Collections.singletonMap(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
    Provisioner provisioner = config.getConfiguredInstance(AnomalyDetectorConfig.PROVISIONER_CLASS_CONFIG,
                                                           Provisioner.class,
                                                           overrideConfigs);
    Supplier<Set<String>> topicNameSupplier = () -> _kafkaCruiseControl.kafkaCluster().topics();
    Set<String> topicNamesMatchedWithPattern = Utils.getTopicNamesMatchedWithPattern(_parameters.topic(), topicNameSupplier);
    String topicName;
    if (topicNamesMatchedWithPattern.size() > 1) {
      throw new IllegalArgumentException(String.format("The RightsizeEndpoint does not support provisioning for multiple topics {%s}.",
                                                       String.join(" ,", topicNamesMatchedWithPattern)));
    } else {
      topicName = topicNamesMatchedWithPattern.iterator().next();
    }
    ProvisionRecommendation recommendation =
        new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(_parameters.numBrokersToAdd())
                                                                              .numPartitions(_parameters.partitionCount())
                                                                              .topic(topicName)
                                                                              .build();
    Map<String, ProvisionRecommendation> provisionRecommendation;
    provisionRecommendation = Collections.singletonMap(RECOMMENDER_UP, recommendation);

    ProvisionerState provisionerState = provisioner.rightsize(provisionRecommendation, new RightsizeOptions());

    return new RightsizeResult(_parameters.numBrokersToAdd(), _parameters.partitionCount(), topicName, provisionerState, config);
  }

  @Override
  public RightsizeParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return RightsizeRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _kafkaCruiseControl = _servlet.asyncKafkaCruiseControl();
    _parameters = (RightsizeParameters) validateNotNull(configs.get(RIGHTSIZE_PARAMETER_OBJECT_CONFIG),
                                                        "Parameter configuration is missing from the request.");
  }
}
