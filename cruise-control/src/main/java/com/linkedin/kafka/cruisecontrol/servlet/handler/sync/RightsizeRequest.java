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
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
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
  protected String _topicName;

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
    ProvisionRecommendation recommendation = createProvisionRecommendation();
    Map<String, ProvisionRecommendation> provisionRecommendation;
    provisionRecommendation = Collections.singletonMap(RECOMMENDER_UP, recommendation);

    ProvisionerState provisionerState = provisioner.rightsize(provisionRecommendation, new RightsizeOptions());

    return new RightsizeResult(_parameters.numBrokersToAdd(), _parameters.partitionCount(), _topicName, provisionerState, config);
  }

  /**
   * Create a provision recommendation and ensure that
   * <ul>
   *   <li>exactly one resource type is set</li>
   *   <li>if the resource type is partition, then the corresponding topic must be specified; otherwise, the topic cannot be specified</li>
   *   <li>if the resource type is partition, then only one topic must be specified</li>
   * </ul>
   *
   * @return The {@link ProvisionRecommendation} to recommend for rightsizing
   * @throws UserRequestException when the sanity check fails.
   */
  private ProvisionRecommendation createProvisionRecommendation() throws UserRequestException {
    ProvisionRecommendation recommendation;

    // Validate multiple resources are not set
    if (_parameters.numBrokersToAdd() != ProvisionRecommendation.DEFAULT_OPTIONAL_INT
        && _parameters.partitionCount() == ProvisionRecommendation.DEFAULT_OPTIONAL_INT) {
      //Validate the topic cannot be specified when the resource type is not partition.
      if (_parameters.topic() != null) {
        throw new UserRequestException("When the resource type is not partition, topic cannot be specified.");
      }
      recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(_parameters.numBrokersToAdd())
                                                                                             .build();
    } else if (_parameters.numBrokersToAdd() == ProvisionRecommendation.DEFAULT_OPTIONAL_INT
               && _parameters.partitionCount() != ProvisionRecommendation.DEFAULT_OPTIONAL_INT) {
      // Validate the topic pattern is not null
      if (_parameters.topic() == null) {
        throw new UserRequestException("When the resource type is partition, the corresponding topic must be specified.");
      }
      // Validate multiple topics were not provided for provisioning
      Supplier<Set<String>> topicNameSupplier = () -> _kafkaCruiseControl.kafkaCluster().topics();
      Set<String> topicNamesMatchedWithPattern = Utils.getTopicNamesMatchedWithPattern(_parameters.topic(), topicNameSupplier);
      if (topicNamesMatchedWithPattern.size() != 1) {
        throw new UserRequestException(String.format("The RightsizeEndpoint does not support provisioning for multiple topics {%s}.",
                                                         String.join(" ,", topicNamesMatchedWithPattern)));
      } else if (topicNamesMatchedWithPattern.iterator().next().isEmpty()) {
        // Validate the topic is not empty
        throw new UserRequestException("When the resource type is partition, the corresponding topic must be specified.");
      } else {
        _topicName = topicNamesMatchedWithPattern.iterator().next();
      }
      recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numPartitions(_parameters.partitionCount())
                                                                                             .topic(_topicName)
                                                                                             .build();
    } else {
      throw new UserRequestException(String.format("Exactly one resource type must be set (Brokers:%d Partitions:%d))",
                                                       _parameters.numBrokersToAdd(),
                                                       _parameters.partitionCount()));
    }
    return recommendation;
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
