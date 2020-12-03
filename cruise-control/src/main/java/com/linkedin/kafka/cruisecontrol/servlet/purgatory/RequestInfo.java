/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.purgatory;

import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;

import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.getClientIpAddress;
import static com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus.*;


/**
 * A class to represent request information in purgatory. Possible status of requests, with supported transitions:
 * <ul>
 *   <li>{@link ReviewStatus#PENDING_REVIEW} -&gt; {@link ReviewStatus#APPROVED}, {@link ReviewStatus#DISCARDED}</li>
 *   <li>{@link ReviewStatus#APPROVED} -&gt; {@link ReviewStatus#DISCARDED}, {@link ReviewStatus#SUBMITTED}</li>
 * </ul>
 */
@JsonResponseClass
public class RequestInfo {
  @JsonResponseField
  public static final String ID = "Id";
  @JsonResponseField
  public static final String SUBMITTER_ADDRESS = "SubmitterAddress";
  @JsonResponseField
  public static final String SUBMISSION_TIME_MS = "SubmissionTimeMs";
  @JsonResponseField
  public static final String STATUS = "Status";
  @JsonResponseField
  public static final String ENDPOINT_WITH_PARAMS = "EndpointWithParams";
  @JsonResponseField
  public static final String REASON = "Reason";
  private static final String INIT_REASON = "Awaiting review.";
  private static final String FINAL_REASON = "Submitted approved request.";
  private static final Map<ReviewStatus, Set<ReviewStatus>> VALID_TRANSFER = new HashMap<>();
  static {
    VALID_TRANSFER.put(PENDING_REVIEW, new HashSet<>(Arrays.asList(APPROVED, DISCARDED)));
    VALID_TRANSFER.put(APPROVED, new HashSet<>(Arrays.asList(DISCARDED, SUBMITTED)));
    VALID_TRANSFER.put(SUBMITTED, Collections.emptySet());
    VALID_TRANSFER.put(DISCARDED, Collections.emptySet());
  }
  private final String _submitterAddress;
  private final long _submissionTimeMs;
  private final Map<String, String[]> _parameterMap;
  private final EndPoint _endPoint;
  private final CruiseControlParameters _parameters;
  private volatile ReviewStatus _status;
  private volatile String _reason;
  private volatile boolean _accessToAlreadySubmittedRequest;

  public <P extends CruiseControlParameters> RequestInfo(HttpServletRequest request, P parameters) {
    if (request == null) {
      throw new IllegalArgumentException("Request is missing from the request info.");
    } else if (parameters == null) {
      throw new IllegalArgumentException("Parameter is missing from the request info.");
    }
    _submitterAddress = getClientIpAddress(request);
    _submissionTimeMs = System.currentTimeMillis();
    _parameterMap = request.getParameterMap();
    _endPoint = ParameterUtils.endPoint(request);
    _parameters = parameters;
    _status = PENDING_REVIEW;
    _reason = INIT_REASON;
    _accessToAlreadySubmittedRequest = false;
  }

  public CruiseControlParameters parameters() {
    return _parameters;
  }

  public String submitterAddress() {
    return _submitterAddress;
  }

  public long submissionTimeMs() {
    return _submissionTimeMs;
  }

  public String reason() {
    return _reason;
  }

  public Map<String, String[]> parameterMap() {
    return _parameterMap;
  }

  /**
   * @return A String that combines the endpoint with parameters.
   */
  public String endpointWithParams() {
    StringBuilder sb = new StringBuilder(_endPoint.toString());
    String queryParamDelimiter = "?";
    for (Map.Entry<String, String[]> paramSet : _parameterMap.entrySet()) {
      for (String paramValue : paramSet.getValue()) {
        sb.append(queryParamDelimiter).append(paramSet.getKey()).append("=").append(paramValue);
        if (queryParamDelimiter.equals("?")) {
          queryParamDelimiter = "&";
        }
      }
    }
    return  sb.toString();
  }

  public EndPoint endPoint() {
    return _endPoint;
  }

  public ReviewStatus status() {
    return _status;
  }

  /**
   * Update review status and the corresponding reason for change to apply the review.
   *
   * @param targetStatus The status after applying the review.
   * @param reason The reason for the status change upon review.
   */
  void applyReview(ReviewStatus targetStatus, String reason) {
    if (!canTransferToStatus(targetStatus)) {
      throw new IllegalStateException("Cannot mark a task in " + _status + " to " + targetStatus + " status. The "
                                      + "valid target statuses are " + validTargetStatus());
    }
    _status = targetStatus;
    _reason = reason;
  }

  /**
   * Submit the review to indicate that it is .
   *
   * @param reviewId The review id for which the corresponding request is requested to be submitted.
   */
  void submitReview(int reviewId) {
    applyReview(SUBMITTED, FINAL_REASON);
    _parameters.setReviewId(reviewId);
  }

  public void setAccessToAlreadySubmittedRequest() {
    _accessToAlreadySubmittedRequest = true;
  }

  public boolean accessToAlreadySubmittedRequest() {
    return _accessToAlreadySubmittedRequest;
  }

  /**
   * Check if the status transfer is possible.
   *
   * @param targetStatus The status to transfer to.
   * @return True if the transfer is valid, false otherwise.
   */
  private boolean canTransferToStatus(ReviewStatus targetStatus) {
    return VALID_TRANSFER.get(_status).contains(targetStatus);
  }

  /**
   * @return The valid target status to transfer to.
   */
  private Set<ReviewStatus> validTargetStatus() {
    return Collections.unmodifiableSet(VALID_TRANSFER.get(_status));
  }

  /**
   * @param reviewId The associate review id with the request.
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure(Integer reviewId) {
    Map<String, Object> jsonMap = new HashMap<>(6);
    jsonMap.put(ID, reviewId);
    jsonMap.put(SUBMITTER_ADDRESS, _submitterAddress);
    jsonMap.put(SUBMISSION_TIME_MS, _submissionTimeMs);
    jsonMap.put(STATUS, _status.toString());
    jsonMap.put(ENDPOINT_WITH_PARAMS, endpointWithParams());
    jsonMap.put(REASON, _reason);
    return jsonMap;
  }
}
