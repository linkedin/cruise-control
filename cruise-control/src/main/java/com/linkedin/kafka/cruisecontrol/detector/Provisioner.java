/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import java.util.Map;
import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * The interface for adding / removing resources to / from the cluster.
 */
@InterfaceStability.Evolving
public interface Provisioner extends CruiseControlConfigurable {

  /**
   * Rightsize the cluster using the given constraints. All given recommendations are expected to share the same {@link ProvisionStatus}.
   * Implementations of this function are expected to be non-blocking -- i.e. starts the rightsizing, but does not block until the completion.
   *
   * <ul>
   *   <li>For {@link ProvisionStatus#UNDER_PROVISIONED} clusters, each recommender (e.g. goal name) indicates requested resources
   *   (e.g. number of brokers) along with relevant constraints (e.g. racks for which brokers should not be added). Typically, aggregating
   *   different recommendations for the same resource type requires considering the maximum value over all recommendations.</li>
   *   <li>For {@link ProvisionStatus#OVER_PROVISIONED} clusters, each recommender (e.g. goal name) indicates resources that can be
   *   released (e.g. number of brokers) along with relevant constraints (e.g. expected broker capacity). Typically, aggregating
   *   different recommendations for the same resource type requires considering the minimum value over all recommendations.</li>
   * </ul>
   *
   * @param recommendationByRecommender Provision recommendations provided by corresponding recommenders.
   * @return {@code true} if actions have been taken on the cluster towards rightsizing, {@code false} otherwise.
   */
  boolean rightsize(Map<String, ProvisionRecommendation> recommendationByRecommender);
}
