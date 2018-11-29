/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.EndPoint;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.handleParameterParseException;


/**
 * An abstract class for Cruise Control parameters. This class will be extended to crete custom parameters for different
 * endpoints.
 */
public abstract class AbstractParameters implements CruiseControlParameters {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractParameters.class);
  protected final HttpServletRequest _request;
  // Common to all parameters, expected to be populated via initParameters.
  protected boolean _json = false;
  protected EndPoint _endPoint = null;

  public AbstractParameters(HttpServletRequest request) {
    _request = request;
  }

  protected void initParameters() throws UnsupportedEncodingException {
    _endPoint = ParameterUtils.endPoint(_request);
    _json = ParameterUtils.wantJSON(_request);
  }

  @Override
  public boolean parseParameters(HttpServletResponse response) {
    try {
      initParameters();
      return false;
    } catch (Exception e) {
      try {
        handleParameterParseException(e, response, e.getMessage(), _json);
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
  public EndPoint endPoint() {
    return _endPoint;
  }
}
