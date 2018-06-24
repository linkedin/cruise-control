/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;

import java.util.function.Function;

/**
 * A factory class of replica sort functions. It is always preferred to
 */
public class ReplicaSortFunctionFactory {

  private ReplicaSortFunctionFactory() {

  }
  /** Prioritize the immigrants replicas */
  private static final Function<Replica, Integer> PRIORITIZE_IMMIGRANTS = r -> r.originalBroker() != r.broker() ? 0 : 1;
  /** De-prioritize the immigrants replicas */
  private static final Function<Replica, Integer> DEPRIORITIZE_IMMIGRANTS = r -> r.originalBroker() != r.broker() ? 1 : 0;
  /** Select leaders only */
  private static final Function<Replica, Boolean> SELECT_LEADERS = Replica::isLeader;

  // Score functions
  /**
   * @param metricName the metric name to score
   * @return a score function to score by the metric value of the given metric name.
   */
  public static Function<Replica, Double> sortByMetricValue(String metricName) {
    return r ->
        r.load().loadByWindows().valuesFor(KafkaMetricDef.commonMetricDef().metricInfo(metricName).id()).avg();
  }

  /**
   * @param metricGroup the metric name to score
   * @return a score function to score by the metric group value of the given metric group.
   */
  public static Function<Replica, Double> sortByMetricGroupValue(String metricGroup) {
    return r -> {
      MetricValues metricValues = r.load()
                                   .loadByWindows()
                                   .valuesForGroup(metricGroup, KafkaMetricDef.commonMetricDef(), true);
      return metricValues.avg();
    };
  }

  // Priority functions
  /**
   * @return a priority function that prioritize the immigrants replicas.
   */
  public static Function<Replica, Integer> prioritizeImmigrants() {
    return PRIORITIZE_IMMIGRANTS;
  }

  /**
   * This priority function can be used together with {@link SortedReplicas#reverselySortedReplicas()}
   * to provide sorted replicas in descending order of score and prioritize the immigrants replicas.
   *
   * @return a priority function that de-prioritize the immigrants replicas
   */
  public static Function<Replica, Integer> deprioritizeImmigrants() {
    return DEPRIORITIZE_IMMIGRANTS;
  }

  // Selection functions

  /**
   * @return a selection function that only include leaders.
   */
  public static Function<Replica, Boolean> selectLeaders() {
    return SELECT_LEADERS;
  }
}
