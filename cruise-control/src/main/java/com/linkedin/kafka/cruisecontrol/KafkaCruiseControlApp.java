/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;

abstract class KafkaCruiseControlApp {

  protected static final String METRIC_DOMAIN = "kafka.cruisecontrol";

  protected final KafkaCruiseControlConfig _config;
  protected final AsyncKafkaCruiseControl _kafkaCruiseControl;
  protected final JmxReporter _jmxReporter;
  protected MetricRegistry _metricRegistry;
  protected Integer _port;
  protected String _hostname;

  KafkaCruiseControlApp(KafkaCruiseControlConfig config, Integer port, String hostname) {
    this._config = config;
    _metricRegistry = new MetricRegistry();
    _jmxReporter = JmxReporter.forRegistry(_metricRegistry).inDomain(METRIC_DOMAIN).build();
    _jmxReporter.start();
    _port = port;
    _hostname = hostname;

    _kafkaCruiseControl = new AsyncKafkaCruiseControl(config, _metricRegistry);

  }

  public String getHostname() {
    return _hostname;
  }

  public int getPort() {
    return _port;
  }

  void start() throws Exception {
    _kafkaCruiseControl.startUp();
  }

  void registerShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
  }

  void stop() {
    _kafkaCruiseControl.shutdown();
    _jmxReporter.close();
  }

  public abstract String serverUrl();
}
