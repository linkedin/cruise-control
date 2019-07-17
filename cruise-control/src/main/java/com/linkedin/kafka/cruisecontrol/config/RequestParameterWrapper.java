/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

/**
 * A wrapper class for pluggable config names to handle a request.
 */
public class RequestParameterWrapper {
  private final String _parametersClass;
  private final String _parametersObject;
  private final String _requestClass;

  public RequestParameterWrapper(String parametersClass, String parametersObject, String requestClass) {
    _parametersClass = parametersClass;
    _parametersObject = parametersObject;
    _requestClass = requestClass;
  }

  public String parametersClass() {
    return _parametersClass;
  }

  public String parameterObject() {
    return _parametersObject;
  }

  public String requestClass() {
    return _requestClass;
  }
}
