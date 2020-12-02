/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;


public final class KafkaCruiseControlServletTestUtils {
  private KafkaCruiseControlServletTestUtils() {

  }

  /**
   * Strip off the '*' from the end of the default web server API URL prefix
   * @return the default web server API URL prefix with no '*' at the end
   */
  public static String getDefaultWebServerApiUrlPrefix() {
    return WebServerConfig.DEFAULT_WEBSERVER_API_URLPREFIX
        .substring(0, WebServerConfig.DEFAULT_WEBSERVER_API_URLPREFIX.length() - 1);
  }
}
