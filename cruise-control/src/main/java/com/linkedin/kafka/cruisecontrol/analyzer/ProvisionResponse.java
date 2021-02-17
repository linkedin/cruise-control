/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


/**
 * Indicates the {@link ProvisionStatus} along with the recommended actions regarding the relevant status.
 * Recommendations are only relevant to {@link ProvisionStatus#UNDER_PROVISIONED} and {@link ProvisionStatus#OVER_PROVISIONED}.
 */
public class ProvisionResponse {
  private ProvisionStatus _status;
  private final StringBuilder _recommendation;

  /**
   * Constructor to be used for provision statuses, for which the recommendations are relevant.
   * Recommendation and recommender are expected to be human-readable strings.
   *
   * @param status The current provision status.
   * @param recommendation Recommended action regarding the given provision status.
   * @param recommender The source of the recommended action to be used in aggregate recommendation.
   */
  public ProvisionResponse(ProvisionStatus status, String recommendation, String recommender) {
    this(status);
    if (!(status == ProvisionStatus.UNDER_PROVISIONED || status == ProvisionStatus.OVER_PROVISIONED)) {
      throw new IllegalArgumentException(String.format("Recommendation is irrelevant for provision status %s.", status));
    }
    validateNotNull(recommendation, "The recommendation cannot be null.");
    validateNotNull(recommender, "The recommender cannot be null.");
    _recommendation.append(String.format("[%s] %s", recommender, recommendation));
  }

  /**
   * Constructor to be used for provision statuses, for which the recommendations are irrelevant.
   */
  public ProvisionResponse(ProvisionStatus status) {
    validateNotNull(status, "The provision status cannot be null.");
    _status = status;
    _recommendation = new StringBuilder();
  }

  /**
   * @return The current provision status.
   */
  public ProvisionStatus status() {
    return _status;
  }

  /**
   * @return Recommended actions regarding the current provision status along with the recommender of each action.
   */
  public String recommendation() {
    return _recommendation.toString();
  }

  /**
   * Aggregate the given provision response to this provision response using the following rules: Aggregating ...
   * <ul>
   *   <li>any provision status with {@link ProvisionStatus#UNDER_PROVISIONED} is {@link ProvisionStatus#UNDER_PROVISIONED}.</li>
   *   <li>a provision status {@code P} with {@link ProvisionStatus#UNDECIDED} is {@code P}.</li>
   *   <li>{@link ProvisionStatus#RIGHT_SIZED} with {@link ProvisionStatus#RIGHT_SIZED} or {@link ProvisionStatus#OVER_PROVISIONED}
   *   is {@link ProvisionStatus#RIGHT_SIZED}.</li>
   *   <li>{@link ProvisionStatus#OVER_PROVISIONED} with {@link ProvisionStatus#OVER_PROVISIONED} yields itself.</li>
   * </ul>
   *
   * Note that these rules enforce that once a state changes from {@link ProvisionStatus#OVER_PROVISIONED} to another state, it cannot go
   * back to this state. Similarly, once a state goes into {@link ProvisionStatus#UNDER_PROVISIONED}, no other followup state is possible.
   * Hence, {@link #_recommendation} for over- or under-provisioned status can be updated without losing relevant information.
   *
   * @param other Provision response to aggregate into this response.
   * @return This provision response after the aggregation.
   */
  public ProvisionResponse aggregate(ProvisionResponse other) {
    if (_status == ProvisionStatus.UNDER_PROVISIONED) {
      if (other.status() == ProvisionStatus.UNDER_PROVISIONED) {
        aggregateRecommendations(other);
      }
    } else {
      switch (other.status()) {
        case UNDER_PROVISIONED:
          _status = ProvisionStatus.UNDER_PROVISIONED;
          clearRecommendation();
          aggregateRecommendations(other);
          break;
        case RIGHT_SIZED:
          _status = ProvisionStatus.RIGHT_SIZED;
          clearRecommendation();
          break;
        case OVER_PROVISIONED:
          if (_status == ProvisionStatus.OVER_PROVISIONED || _status == ProvisionStatus.UNDECIDED) {
            _status = ProvisionStatus.OVER_PROVISIONED;
            aggregateRecommendations(other);
            break;
          }
          // Keep the status as right-sized if it was right-sized before.
          break;
        case UNDECIDED:
          // Nothing to do.
          break;
        default:
          throw new IllegalArgumentException("Unsupported provision status " + other + " is provided.");
      }
    }
    return this;
  }

  private void clearRecommendation() {
    if (_recommendation.length() > 0) {
      _recommendation.setLength(0);
    }
  }

  private void aggregateRecommendations(ProvisionResponse other) {
    String otherRecommendation = other.recommendation();
    if (!otherRecommendation.isEmpty()) {
      _recommendation.append(_recommendation.length() == 0 ? "" : " ").append(otherRecommendation);
    }
  }

  @Override
  public String toString() {
    return String.format("%s%s", _status, _recommendation.length() == 0 ? "" : String.format(" (%s)", recommendation()));
  }
}
