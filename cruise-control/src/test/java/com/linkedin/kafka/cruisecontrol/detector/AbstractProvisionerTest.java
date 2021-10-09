/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.Map;
import org.easymock.EasyMock;
import org.junit.Before;

import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfigUtils.getConfiguredInstance;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.BasicProvisioner.BROKER_PROVISIONER_CLASS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.BasicProvisioner.DEFAULT_BROKER_PROVISIONER_CLASS;
import static com.linkedin.kafka.cruisecontrol.detector.BasicProvisioner.DEFAULT_PARTITION_PROVISIONER_CLASS;
import static com.linkedin.kafka.cruisecontrol.detector.BasicProvisioner.PARTITION_PROVISIONER_CLASS_CONFIG;


public abstract class AbstractProvisionerTest {
  protected static final KafkaCruiseControl MOCK_KAFKA_CRUISE_CONTROL = EasyMock.createMock(KafkaCruiseControl.class);
  protected static final Map<String, Object> PROVISIONER_CONFIGS = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG,
                                                                          MOCK_KAFKA_CRUISE_CONTROL,
                                                                          BROKER_PROVISIONER_CLASS_CONFIG,
                                                                          DEFAULT_BROKER_PROVISIONER_CLASS.getName(),
                                                                          PARTITION_PROVISIONER_CLASS_CONFIG,
                                                                          DEFAULT_PARTITION_PROVISIONER_CLASS.getName());
  protected static final ProvisionRecommendation BROKER_REC_TO_EXECUTE = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
      .numBrokers(2).resource(Resource.CPU).build();
  public static final String RECOMMENDER_TO_EXECUTE = "ToExecute";
  protected static final RightsizeOptions RIGHTSIZE_OPTIONS = new RightsizeOptions();
  protected Provisioner _provisioner;

  /**
   * Execute before every test case.
   */
  @Before
  public void setUp() {
    _provisioner = getConfiguredInstance(BasicProvisioner.class, Provisioner.class, PROVISIONER_CONFIGS);
  }
}
