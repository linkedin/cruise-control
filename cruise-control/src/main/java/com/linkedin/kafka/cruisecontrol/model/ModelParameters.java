/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import java.util.Collection;
import org.apache.kafka.common.record.CompressionType;


public class ModelParameters {
  // The linear regression model parameters.
  private static final LinearRegressionModelParameters LINEAR_REGRESSION_PARAMETERS = new LinearRegressionModelParameters();

  // The static model
  /**
   * The contribution weight of leader bytes in on the CPU utilization of a broker.
   */
  static double CPU_WEIGHT_OF_LEADER_BYTES_IN_RATE = 0.7;
  /**
   * The contribution weight of leader bytes out on the CPU utilization of a broker.
   */
  static double CPU_WEIGHT_OF_LEADER_BYTES_OUT_RATE = 0.15;
  /**
   * The contribution weight of follower bytes in on the CPU utilization of a broker.
   */
  static double CPU_WEIGHT_OF_FOLLOWER_BYTES_IN_RATE = 0.15;

  private ModelParameters() {

  }

  public static void init(KafkaCruiseControlConfig config) {
    CPU_WEIGHT_OF_LEADER_BYTES_IN_RATE =
        config.getDouble(KafkaCruiseControlConfig.LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG);
    CPU_WEIGHT_OF_LEADER_BYTES_OUT_RATE =
        config.getDouble(KafkaCruiseControlConfig.LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG);
    CPU_WEIGHT_OF_FOLLOWER_BYTES_IN_RATE =
        config.getDouble(KafkaCruiseControlConfig.FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG);
    LINEAR_REGRESSION_PARAMETERS.init(config);
  }

  public static Double getCoefficient(LinearRegressionModelParameters.ModelCoefficient name) {
    return LINEAR_REGRESSION_PARAMETERS.getCoefficient(name);
  }

  public static boolean trainingCompleted() {
    return LINEAR_REGRESSION_PARAMETERS.trainingCompleted();
  }

  public static double modelCoefficientTrainingCompleteness() {
    return LINEAR_REGRESSION_PARAMETERS.modelCoefficientTrainingCompleteness();
  }

  /**
   * Trigger the calculation of the model parameters.
   * @return true if the parameters are generated, otherwise false;
   */
  public static boolean updateModelCoefficient() {
    return LINEAR_REGRESSION_PARAMETERS.updateModelCoefficient();
  }

  public static void addMetricObservation(Collection<BrokerMetricSample> trainingData) {
    LINEAR_REGRESSION_PARAMETERS.addMetricObservation(trainingData);
  }

  public static LinearRegressionModelParameters.LinearRegressionModelState linearRegressionModelState() {
    return LINEAR_REGRESSION_PARAMETERS.modelState();
  }

  // The following methods are not used at this point. They are supposed to be used for static model when users did
  // not specify the weights.
  private static ConfigSetting forSetting(CompressionType type, boolean sslEnabled) {
    switch (type) {
      case NONE:
        return sslEnabled ? ConfigSetting.SSL_NONE : ConfigSetting.PLAINTEXT_NONE;
      case GZIP:
        return sslEnabled ? ConfigSetting.SSL_GZIP : ConfigSetting.PLAINTEXT_GZIP;
      case SNAPPY:
        return sslEnabled ? ConfigSetting.SSL_SNAPPY : ConfigSetting.PLAINTEXT_SNAPPY;
      case LZ4:
        return sslEnabled ? ConfigSetting.SSL_LZ4 : ConfigSetting.PLAINTEXT_LZ4;
      default:
        throw new IllegalStateException("Should not be here.");
    }
  }

  /**
   * An enumeration holding the different configuration combinations. CURRENT_CLUSTER refers to the current cluster's
   * configuration.
   */
  public enum ConfigSetting {
    CURRENT_CLUSTER,
    PLAINTEXT_NONE, PLAINTEXT_GZIP, PLAINTEXT_SNAPPY, PLAINTEXT_LZ4,
    SSL_NONE, SSL_GZIP, SSL_SNAPPY, SSL_LZ4
  }
}
