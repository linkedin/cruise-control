/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.purgatory;

import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.EndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReviewResult;
import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.EndPoint.REVIEW;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.httpServletRequestToString;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.POST_METHOD;


/**
 * A Class to keep POST requests that are awaiting review if two-step verification is enabled.
 *
 * The Purgatory is thread-safe.
 */
public class Purgatory implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Purgatory.class);
  private static final long PURGATORY_CLEANER_PERIOD_SECONDS = 10;
  private static final long PURGATORY_CLEANER_INITIAL_DELAY_SECONDS = 0;
  private int _requestId;
  private final long _purgatoryRetentionTimeMs;
  private final Map<Integer, RequestInfo> _requestInfoById;
  private final ScheduledExecutorService _purgatoryCleaner =
      Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("PurgatoryCleaner", true, null));
  private final KafkaCruiseControlConfig _config;

  public Purgatory(KafkaCruiseControlConfig config) {
    _requestId = 0;
    _config = config;
    _purgatoryRetentionTimeMs = config.getLong(KafkaCruiseControlConfig.TWO_STEP_PURGATORY_RETENTION_TIME_MS_CONFIG);
    int purgatoryMaxCachedRequests = config.getInt(KafkaCruiseControlConfig.TWO_STEP_PURGATORY_MAX_REQUESTS_CONFIG);

    _requestInfoById = new LinkedHashMap<Integer, RequestInfo>() {
      @Override
      protected boolean removeEldestEntry(Map.Entry<Integer, RequestInfo> eldest) {
        return this.size() > purgatoryMaxCachedRequests;
      }
    };

    _purgatoryCleaner.scheduleAtFixedRate(new PurgatoryCleaner(),
                                          PURGATORY_CLEANER_INITIAL_DELAY_SECONDS,
                                          PURGATORY_CLEANER_PERIOD_SECONDS,
                                          TimeUnit.SECONDS);
  }

  /**
   * Add request to the purgatory and return the {@link ReviewResult} for the request that has been added to
   * the purgatory.
   *
   * @param request Http Servlet Request to add to the purgatory.
   * @param parameters Request parameters.
   * @param <P> Type corresponding to the request parameters.
   * @return The result showing the {@link ReviewResult} for the request that has been added to the purgatory.
   */
  public synchronized <P extends CruiseControlParameters> ReviewResult addRequest(HttpServletRequest request,
                                                                                  P parameters) {
    if (!request.getMethod().equals(POST_METHOD)) {
      throw new IllegalArgumentException(String.format("Purgatory can only contain POST request (Attempted to add: %s).",
                                                       httpServletRequestToString(request)));
    }
    RequestInfo requestInfo = new RequestInfo(request, parameters);
    _requestInfoById.put(_requestId, requestInfo);

    Map<Integer, RequestInfo> requestInfoById = new HashMap<>();
    requestInfoById.put(_requestId, requestInfo);
    Set<Integer> filteredRequestIds = new HashSet<>();
    filteredRequestIds.add(_requestId);

    ReviewResult result = new ReviewResult(requestInfoById, filteredRequestIds, _config);
    _requestId++;
    return result;
  }

  /**
   * Ensure that:
   * <ul>
   *   <li>A request with the given review id exists in the purgatory.</li>
   *   <li>The request with the given review id matches the given request.</li>
   *   <li>The request with the given review id is approved in the purgatory.</li>
   * </ul>
   *
   * Then mark the review status as submitted.
   *
   * @param reviewId The review id for which the corresponding request is requested to be submitted.
   * @param request The request to submit.
   * @return Submitted request info.
   */
  public synchronized RequestInfo submit(int reviewId, HttpServletRequest request) {
    RequestInfo requestInfo = _requestInfoById.get(reviewId);
    // 1. Ensure that a request with the given review id exists in the purgatory.
    if (requestInfo == null) {
      throw new UserRequestException(
          String.format("No request with review id %d exists in purgatory. Please use %s endpoint to check for the "
                        + "current requests awaiting review in purgatory.", reviewId, REVIEW));
    }

    // 2. Ensure that the request with the given review id matches the given request.
    EndPoint endpoint = ParameterUtils.endPoint(request);
    if (requestInfo.endPoint() != endpoint) {
      throw new UserRequestException(
          String.format("Request with review id %d is associated with %s endpoint, but the given request has %s endpoint."
                        + "Please use %s endpoint to check for the current requests awaiting review in purgatory.",
                        reviewId, requestInfo.endPoint(), endpoint, REVIEW));
    }

    if (requestInfo.status() == ReviewStatus.SUBMITTED) {
        LOG.info("Request {} has already been submitted (review: {}).", requestInfo.endpointWithParams(), reviewId);
        requestInfo.setAccessToAlreadySubmittedRequest();
    } else {
      // 3. Ensure that the request with the given review id is approved in the purgatory, and mark the status as submitted.
      requestInfo.submitReview(reviewId);
      LOG.info("Submitted request {} for execution (review: {}).", requestInfo.endpointWithParams(), reviewId);
    }
    return requestInfo;
  }

  /**
   * Remove the {@link ReviewStatus#SUBMITTED} request associated with the given review id from purgatory.
   *
   * @param reviewId Review id of the request to be removed from the purgatory.
   * @return Removed submitted request if exists in purgatory, null otherwise.
   */
  public synchronized RequestInfo removeSubmitted(int reviewId) {
    RequestInfo requestInfo = _requestInfoById.get(reviewId);
    if (requestInfo == null) {
      return null;
    } else if (requestInfo.status() != ReviewStatus.SUBMITTED) {
      throw new IllegalStateException(
          String.format("Attempt to remove request associated with review id %d from purgatory. Status (current %s, "
                        + "expected: %s).", reviewId, requestInfo.status(), ReviewStatus.SUBMITTED));
    }

    return _requestInfoById.remove(reviewId);
  }

  /**
   * Get requested reviews from the review board.
   *
   * @param reviewIds Requests for which the result is requested, empty set implies all requests in the review board.
   * @return The requested reviews from the review board.
   */
  public synchronized ReviewResult reviewBoard(Set<Integer> reviewIds) {
    return new ReviewResult(new HashMap<>(_requestInfoById), reviewIds, _config);
  }

  /**
   * Apply the given target states to review the corresponding requests. Get the post-review result of the purgatory.
   *
   * @param requestIdsByTargetState Request Ids by target review state for requests in purgatory.
   * @param reason Common reason for applying the review to the requests.
   * @return The result showing the current purgatory state after the review.
   */
  public synchronized ReviewResult applyReview(Map<ReviewStatus, Set<Integer>> requestIdsByTargetState, String reason) {
    // Sanity check if all request ids in the review exists in the purgatory.
    Set<Integer> reviewedRequestIds = new HashSet<>();
    for (Map.Entry<ReviewStatus, Set<Integer>> entry : requestIdsByTargetState.entrySet()) {
      Set<Integer> requestIds = entry.getValue();
      if (!_requestInfoById.keySet().containsAll(requestIds)) {
        throw new IllegalStateException(String.format("Review contains request ids (%s) that do not exist in purgatory.",
                                                      requestIds.removeAll(_requestInfoById.keySet())));
      }
      // Apply review to each Request Info
      ReviewStatus targetReviewStatus = entry.getKey();
      requestIds.forEach(requestId -> _requestInfoById.get(requestId).applyReview(targetReviewStatus, reason));
      reviewedRequestIds.addAll(requestIds);
    }

    // Return the post-review result of the purgatory.
    return new ReviewResult(new HashMap<>(_requestInfoById), reviewedRequestIds, _config);
  }

  private synchronized void removeOldRequests() {
    LOG.debug("Remove old requests from purgatory.");
    _requestInfoById.entrySet().removeIf(entry -> (entry.getValue().submissionTimeMs()
                                                   + _purgatoryRetentionTimeMs < System.currentTimeMillis()));
  }

  @Override
  public void close() {
    _purgatoryCleaner.shutdownNow();
    _requestInfoById.clear();
  }

  /**
   * A runnable class to remove expired requests.
   */
  private class PurgatoryCleaner implements Runnable {
    @Override
    public void run() {
      try {
        removeOldRequests();
      } catch (Throwable t) {
        LOG.warn("Received exception when trying to remove old requests from purgatory.", t);
      }
    }
  }
}
