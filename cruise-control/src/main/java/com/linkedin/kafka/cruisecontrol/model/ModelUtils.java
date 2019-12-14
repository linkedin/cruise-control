/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The util class for model.
 */
public class ModelUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ModelUtils.class);
  public static final String BROKER_ID = "brokerid";
  public static final String BROKER_STATE = "brokerstate";
  public static final String REPLICAS = "replicas";
  public static final String IS_LEADER = "isLeader";
  public static final String TOPIC = "topic";
  public static final String PARTITION = "partition";
  public static final String LOAD = "load";
  public static final String METRIC_VALUES = "MetricValues";
  public static final String NAME = "name";
  public static final String BROKERS = "brokers";
  public static final String RACK_ID = "rackid";
  public static final String HOSTS = "hosts";
  // In some extremely low throughput cluster, the partition IO may appear to be higher than the broker IO.
  // We allow such error if it is not too big and just treat the partition IO the same as broker IO.
  // We also ignore all the inaccuracy when the broker total throughput is too low.
  private static final double ALLOWED_METRIC_ERROR_FACTOR = 1.05;
  private static final int UNSTABLE_METRIC_THROUGHPUT_THRESHOLD = 10;
  private static boolean _useLinearRegressionModel = false;

  private ModelUtils() {

  }

  public static void init(KafkaCruiseControlConfig config) {
    _useLinearRegressionModel = config.getBoolean(MonitorConfig.USE_LINEAR_REGRESSION_MODEL_CONFIG);
  }

  /**
   * Get the CPU utilization of follower using the load of the leader replica.
   *
   * @param leaderBytesInRate Leader bytes in rate.
   * @param leaderBytesOutRate Leader bytes out rate.
   * @param leaderCpuUtil CPU utilization of the leader.
   * @return The CPU utilization of follower using the load of the leader replica.
   */
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

  /**
   * Estimate the leader CPU utilization for the partition with the given information as a Double in [0.0,1.0], or
   * {@code null} if estimation is not possible due to inconsistency between partition and broker-level byte rates.
   *
   * @param brokerCpuUtil A double in [0.0,1.0], representing the CPU usage of the broker hosting the partition leader.
   * @param brokerLeaderBytesInRate Leader bytes in rate in the broker.
   * @param brokerLeaderBytesOutRate  Leader bytes out rate in the broker.
   * @param brokerFollowerBytesInRate Follower bytes in rate in the broker.
   * @param partitionBytesInRate Leader bytes in rate for the partition.
   * @param partitionBytesOutRate Total bytes out rate (i.e. leader/replication bytes out) for the partition.
   * @return Estimated CPU utilization of the leader replica of the partition as a Double in [0.0,1.0], or {@code null}
   * if estimation is not possible due to inconsistency between partition and broker-level byte rates.
   */
  public static Double estimateLeaderCpuUtilPerCore(double brokerCpuUtil,
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
        LOG.error("Partition bytes in rate {} is greater than broker bytes in rate {}.",
                  partitionBytesInRate, brokerLeaderBytesInRate);
        return null;
      } else if (brokerLeaderBytesOutRate * ALLOWED_METRIC_ERROR_FACTOR < partitionBytesOutRate
                 && brokerLeaderBytesOutRate > UNSTABLE_METRIC_THROUGHPUT_THRESHOLD) {
        LOG.error("Partition bytes out rate {} is greater than broker bytes out rate {}.",
                  partitionBytesOutRate, brokerLeaderBytesOutRate);
        return null;
      } else {
        double brokerLeaderBytesInContribution = ModelParameters.CPU_WEIGHT_OF_LEADER_BYTES_IN_RATE * brokerLeaderBytesInRate;
        double brokerLeaderBytesOutContribution = ModelParameters.CPU_WEIGHT_OF_LEADER_BYTES_OUT_RATE * brokerLeaderBytesOutRate;
        double brokerFollowerBytesInContribution = ModelParameters.CPU_WEIGHT_OF_FOLLOWER_BYTES_IN_RATE * brokerFollowerBytesInRate;
        double totalContribution = brokerLeaderBytesInContribution + brokerLeaderBytesOutContribution + brokerFollowerBytesInContribution;
        double leaderReplicaContribution = (brokerLeaderBytesInContribution * Math.min(1, partitionBytesInRate / brokerLeaderBytesInRate))
                                           + (brokerLeaderBytesOutContribution * Math.min(1, partitionBytesOutRate / brokerLeaderBytesOutRate));

        return (leaderReplicaContribution / totalContribution) * brokerCpuUtil;
      }
    }
  }

  /**
   * Estimate the leader CPU utilization using linear regression model.
   *
   * @param leaderBytesInRate Leader bytes in rate.
   * @param leaderBytesOutRate Leader bytes out rate.
   * @return Estimated leader CPU utilization.
   */
  public static double estimateLeaderCpuUtilUsingLinearRegressionModel(double leaderBytesInRate,
                                                                       double leaderBytesOutRate) {
    double leaderBytesInCoefficient =
        ModelParameters.getCoefficient(LinearRegressionModelParameters.ModelCoefficient.LEADER_BYTES_IN);
    double leaderBytesOutCoefficient =
        ModelParameters.getCoefficient(LinearRegressionModelParameters.ModelCoefficient.LEADER_BYTES_OUT);
    return leaderBytesInCoefficient * leaderBytesInRate
        + leaderBytesOutCoefficient * leaderBytesOutRate;
  }
}
