/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.RequestInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.DATE_FORMAT;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.TIME_ZONE;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;


public class ReviewResult extends AbstractCruiseControlResponse {
  private static final String ID = "Id";
  private static final String SUBMITTER_ADDRESS = "SubmitterAddress";
  private static final String SUBMISSION_TIME_MS = "SubmissionTimeMs";
  private static final String STATUS = "Status";
  private static final String ENDPOINT_WITH_PARAMS = "EndpointWithParams";
  private static final String REASON = "Reason";
  private static final String REQUEST_INFO = "RequestInfo";
  private final Map<Integer, RequestInfo> _requestInfoById;
  private final Set<Integer> _filteredRequestIds;

  /**
   * @param requestInfoById Request info by Id.
   * @param filteredRequestIds Requests for which the result is requested, empty set implies all requests in review board
   */
  public ReviewResult(Map<Integer, RequestInfo> requestInfoById,
                      Set<Integer> filteredRequestIds,
                      KafkaCruiseControlConfig config) {
    super(config);
    _requestInfoById = requestInfoById;
    _filteredRequestIds = filteredRequestIds.isEmpty() ? _requestInfoById.keySet() : filteredRequestIds;
  }

  private String getPlaintext() {
    StringBuilder sb = new StringBuilder();
    int padding = 2;
    // Plaintext response, each column name will be underscore-separated instead of case-separated.
    int idLabelSize = ID.length();
    int submitterAddressLabelSize = SUBMITTER_ADDRESS.length() + 1;
    int submissionTimeLabelSize = SUBMISSION_TIME_MS.length() - 2; // Returns submission_time -- i.e. not submission_time_ms.
    int statusLabelSize = STATUS.length();
    int endpointWithParamsLabelSize = ENDPOINT_WITH_PARAMS.length() + 2;
    int reasonLabelSize = REASON.length();

    for (Map.Entry<Integer, RequestInfo> entry : _requestInfoById.entrySet()) {
      if (_filteredRequestIds.contains(entry.getKey())) {
        // ID
        idLabelSize = Math.max(idLabelSize, (int) (Math.log10(entry.getKey()) + 1));
        RequestInfo requestInfo = entry.getValue();
        // SUBMITTER_ADDRESS
        submitterAddressLabelSize = Math.max(submitterAddressLabelSize, requestInfo.submitterAddress().length());
        // SUBMISSION_TIME_MS
        String dateFormatted = KafkaCruiseControlUtils.toDateString(requestInfo.submissionTimeMs(), DATE_FORMAT, TIME_ZONE);
        submissionTimeLabelSize = Math.max(submissionTimeLabelSize, dateFormatted.length());
        // STATUS
        statusLabelSize = Math.max(statusLabelSize, requestInfo.status().toString().length());
        // ENDPOINT_WITH_PARAMS
        endpointWithParamsLabelSize = Math.max(endpointWithParamsLabelSize, requestInfo.endpointWithParams().length());
        // REASON
        reasonLabelSize = Math.max(reasonLabelSize, requestInfo.reason().length());
      }
    }

    // Populate header.
    StringBuilder formattingStringBuilder = new StringBuilder("%n%-");
    formattingStringBuilder.append(idLabelSize + padding)
                           .append("s%-")
                           .append(submitterAddressLabelSize + padding)
                           .append("s%-")
                           .append(submissionTimeLabelSize + padding)
                           .append("s%-")
                           .append(statusLabelSize + padding)
                           .append("s%-")
                           .append(endpointWithParamsLabelSize + padding)
                           .append("s%-")
                           .append(reasonLabelSize + padding)
                           .append("s");
    sb.append(String.format(formattingStringBuilder.toString(), "ID", "SUBMITTER_ADDRESS", "SUBMISSION_TIME", "STATUS",
                            "ENDPOINT_WITH_PARAMS", "REASON"));

    // Populate values.
    for (Map.Entry<Integer, RequestInfo> entry : _requestInfoById.entrySet()) {
      if (_filteredRequestIds.contains(entry.getKey())) {
        RequestInfo requestInfo = entry.getValue();
        String dateFormatted = KafkaCruiseControlUtils.toDateString(requestInfo.submissionTimeMs(), DATE_FORMAT, TIME_ZONE);
        sb.append(String.format(formattingStringBuilder.toString(), entry.getKey(), requestInfo.submitterAddress(),
                                dateFormatted, requestInfo.status(), requestInfo.endpointWithParams(), requestInfo.reason()));
      }
    }

    return sb.toString();
  }

  private String getJSONString() {
    List<Map<String, Object>> jsonRequestInfoList = new ArrayList<>(_filteredRequestIds.size());
    for (Map.Entry<Integer, RequestInfo> entry : _requestInfoById.entrySet()) {
      if (_filteredRequestIds.contains(entry.getKey())) {
        addJSONRequestInfo(jsonRequestInfoList, entry);
      }
    }
    Map<String, Object> jsonResponse = new HashMap<>(2);
    jsonResponse.put(REQUEST_INFO, jsonRequestInfoList);
    jsonResponse.put(VERSION, JSON_VERSION);
    return new Gson().toJson(jsonResponse);
  }

  private void addJSONRequestInfo(List<Map<String, Object>> jsonRequestInfoList, Map.Entry<Integer, RequestInfo> entry) {
    Map<String, Object> jsonObjectMap = new HashMap<>();
    RequestInfo requestInfo = entry.getValue();
    jsonObjectMap.put(ID, entry.getKey());
    jsonObjectMap.put(SUBMITTER_ADDRESS, requestInfo.submitterAddress());
    jsonObjectMap.put(SUBMISSION_TIME_MS, requestInfo.submissionTimeMs());
    jsonObjectMap.put(STATUS, requestInfo.status().toString());
    jsonObjectMap.put(ENDPOINT_WITH_PARAMS, requestInfo.endpointWithParams());
    jsonObjectMap.put(REASON, requestInfo.reason());
    jsonRequestInfoList.add(jsonObjectMap);
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJSONString() : getPlaintext();
    // Discard irrelevant response.
    _requestInfoById.clear();
    _filteredRequestIds.clear();
  }
}
