/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;


/**
 * The util class for model.
 */
public class ModelUtils {
  // In some extremely low throughput cluster, the partition IO may appear to be higher than the broker IO.
  // We allow such error if it is not too big and just treat the partition IO the same as broker IO.
  // We also ignore all the inaccuracy when the broker total throughput is too low.
  private static final double ALLOWED_METRIC_ERROR_FACTOR = 1.05;
  private static final int UNSTABLE_METRIC_THROUGHPUT_THRESHOLD = 10;
  private static boolean _useLinearRegressionModel = false;

  private ModelUtils() {

  }

  public static void init(KafkaCruiseControlConfig config) {
    _useLinearRegressionModel = config.getBoolean(KafkaCruiseControlConfig.USE_LINEAR_REGRESSION_MODEL_CONFIG);
  }

  public static double getFollowerCpuUtilFromLeaderLoad(double leaderBytesInRate,
                                                        double leaderBytesOutRate,
                                                        double leaderCpuUtil) {
    if (_useLinearRegressionModel) {
      double followerBytesInCoefficient =
          ModelParameters.getCoefficient(LinearRegressionModelParameters.ModelCoefficient.FOLLOWER_BYTES_IN);
      return followerBytesInCoefficient * leaderBytesInRate;
    } else {
      if (leaderBytesInRate == 0.0 && leaderBytesOutRate == 0.0) {
        return 0.0;
      } else {
        return leaderCpuUtil * (ModelParameters.CPU_WEIGHT_OF_FOLLOWER_BYTES_IN_RATE * leaderBytesInRate) / (
            ModelParameters.CPU_WEIGHT_OF_LEADER_BYTES_IN_RATE * leaderBytesInRate
                + ModelParameters.CPU_WEIGHT_OF_LEADER_BYTES_OUT_RATE * leaderBytesOutRate);
      }
    }
  }

  public static double estimateLeaderCpuUtil(double brokerCpuUtil,
                                             double brokerLeaderBytesInRate,
                                             double brokerLeaderBytesOutRate,
                                             double brokerFollowerBytesInRate,
                                             double partitionBytesInRate,
                                             double partitionBytesOutRate) {
    if (_useLinearRegressionModel) {
      return estimateLeaderCpuUtilUsingLinearRegressionModel(partitionBytesInRate, partitionBytesOutRate);
    } else {
      if (brokerLeaderBytesInRate == 0 || brokerLeaderBytesOutRate == 0) {
        return 0.0;
      } else if (brokerLeaderBytesInRate * ALLOWED_METRIC_ERROR_FACTOR < partitionBytesInRate
          && brokerLeaderBytesInRate > UNSTABLE_METRIC_THROUGHPUT_THRESHOLD) {
        throw new IllegalArgumentException(String.format("The partition bytes in rate %f is greater than the broker "
            + "bytes in rate %f", partitionBytesInRate, brokerLeaderBytesInRate));
      } else if (brokerLeaderBytesOutRate * ALLOWED_METRIC_ERROR_FACTOR < partitionBytesOutRate
          && brokerLeaderBytesOutRate > UNSTABLE_METRIC_THROUGHPUT_THRESHOLD) {
        throw new IllegalArgumentException(String.format("The partition bytes out rate %f is greater than the broker "
            + "bytes out rate %f", partitionBytesOutRate, brokerLeaderBytesOutRate));
      } else {
        double brokerLeaderBytesInContribution = ModelParameters.CPU_WEIGHT_OF_LEADER_BYTES_IN_RATE * brokerLeaderBytesInRate;
        double brokerLeaderBytesOutContribution = ModelParameters.CPU_WEIGHT_OF_LEADER_BYTES_OUT_RATE * brokerLeaderBytesOutRate;
        double brokerFollowerBytesInContribution = ModelParameters.CPU_WEIGHT_OF_FOLLOWER_BYTES_IN_RATE * brokerFollowerBytesInRate;
        double totalContribution = brokerLeaderBytesInContribution + brokerLeaderBytesOutContribution + brokerFollowerBytesInContribution;

        double leaderBytesInCpuContribution = brokerCpuUtil * (brokerLeaderBytesInContribution / totalContribution);
        double leaderBytesOutCpuContribution = brokerCpuUtil * (brokerLeaderBytesOutContribution / totalContribution);

        return leaderBytesInCpuContribution * Math.min(1, partitionBytesInRate / brokerLeaderBytesInRate)
            + leaderBytesOutCpuContribution * Math.min(1, partitionBytesOutRate / brokerLeaderBytesOutRate);
      }
    }
  }

  public static double estimateLeaderCpuUtilUsingLinearRegressionModel(double leaderPartitionBytesInRate,
                                                                       double leaderPartitionBytesOutRate) {
    double leaderBytesInCoefficient =
        ModelParameters.getCoefficient(LinearRegressionModelParameters.ModelCoefficient.LEADER_BYTES_IN);
    double leaderBytesOutCoefficient =
        ModelParameters.getCoefficient(LinearRegressionModelParameters.ModelCoefficient.LEADER_BYTES_OUT);
    return leaderBytesInCoefficient * leaderPartitionBytesInRate
        + leaderBytesOutCoefficient * leaderPartitionBytesOutRate;
  }
}
