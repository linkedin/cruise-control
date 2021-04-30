/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.KAFKA_CRUISE_CONTROL_CONFIG_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.KAFKA_CRUISE_CONTROL_HTTP_SERVLET_REQUEST_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DO_AS;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.handleParameterParseException;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.JSON_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.GET_RESPONSE_SCHEMA;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


/**
 * An abstract class for Cruise Control parameters. This class will be extended to crete custom parameters for different
 * endpoints.
 */
public abstract class AbstractParameters implements CruiseControlParameters {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractParameters.class);
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(JSON_PARAM);
    validParameterNames.add(GET_RESPONSE_SCHEMA);
    validParameterNames.add(DO_AS);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);

  }
  protected HttpServletRequest _request;
  protected boolean _initialized = false;
  protected KafkaCruiseControlConfig _config;
  // Common to all parameters, expected to be populated via initParameters.
  protected boolean _json = false;
  protected boolean _wantResponseSchema = false;
  protected EndPoint _endPoint = null;

  public AbstractParameters() {

  }

  protected void initParameters() throws UnsupportedEncodingException {
    _initialized = true;
    _endPoint = ParameterUtils.endPoint(_request);
    _json = ParameterUtils.wantJSON(_request);
    _wantResponseSchema = ParameterUtils.wantResponseSchema(_request);
  }

  @Override
  public boolean parseParameters(HttpServletResponse response) {
    if (_initialized) {
      LOG.trace("Attempt to parse an already parsed request {}.", _request);
      return false;
    }
    try {
      initParameters();
      return false;
    } catch (Exception e) {
      try {
        handleParameterParseException(e, response, e.getMessage(), _json, _wantResponseSchema, _config);
      } catch (IOException ioe) {
        LOG.error(String.format("Failed to write parse parameter exception to output stream. Endpoint: %s.", _endPoint), ioe);
      }
      return true;
    }
  }

  @Override
  public boolean json() {
    return _json;
  }

  @Override
  public boolean wantResponseSchema() {
    return _wantResponseSchema;
  }

  @Override
  public void setReviewId(int reviewId) {
    // Relevant to parameters with review process.
  }

  @Override
  public EndPoint endPoint() {
    return _endPoint;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _request = (HttpServletRequest) validateNotNull(configs.get(KAFKA_CRUISE_CONTROL_HTTP_SERVLET_REQUEST_OBJECT_CONFIG),
                                                    "HttpServletRequest configuration is missing from the request.");
    _config = (KafkaCruiseControlConfig) validateNotNull(configs.get(KAFKA_CRUISE_CONTROL_CONFIG_OBJECT_CONFIG),
                                                         "KafkaCruiseControlConfig configuration is missing from the request.");
  }
}
