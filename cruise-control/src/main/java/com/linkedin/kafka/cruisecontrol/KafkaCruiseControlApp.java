/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

public class KafkaCruiseControlApp {

  private static final String METRIC_DOMAIN = "kafka.cruisecontrol";

  private final Server _server;
  private final KafkaCruiseControlConfig _config;
  private final AsyncKafkaCruiseControl _kafkaCruiseControl;
  private final JmxReporter _jmxReporter;

  KafkaCruiseControlApp(String configPath, Integer port, String hostname) throws IOException {
    this(readConfig(configPath), port, hostname);
  }

  KafkaCruiseControlApp(KafkaCruiseControlConfig config, Integer port, String hostname) {
    this._config = config;

    int actualPort;
    if (port != null) {
      actualPort = port;
    } else {
      actualPort = config.getInt(WebServerConfig.WEBSERVER_HTTP_PORT_CONFIG);
    }

    String actualHostname;
    if (hostname != null) {
      actualHostname = hostname;
    } else {
      actualHostname = config.getString(WebServerConfig.WEBSERVER_HTTP_ADDRESS_CONFIG);
    }

    MetricRegistry metricRegistry = new MetricRegistry();
    _jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain(METRIC_DOMAIN).build();
    _jmxReporter.start();

    _kafkaCruiseControl = new AsyncKafkaCruiseControl(config, metricRegistry);

    _server = new Server(new InetSocketAddress(actualHostname, actualPort));
    NCSARequestLog requestLog = createRequestLog();
    if (requestLog != null) {
      _server.setRequestLog(requestLog);
    }

    ServletContextHandler contextHandler = createContextHandler();
    _server.setHandler(contextHandler);

    setupWebUi(contextHandler);

    KafkaCruiseControlServlet servlet = new KafkaCruiseControlServlet(_kafkaCruiseControl, metricRegistry);
    String apiUrlPrefix = config.getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG);
    ServletHolder servletHolder = new ServletHolder(servlet);
    contextHandler.addServlet(servletHolder, apiUrlPrefix);
  }

  void start() throws Exception {
    _kafkaCruiseControl.startUp();
    _server.start();
    printStartupInfo();
  }

  void registerShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
  }

  void stop() {
    _kafkaCruiseControl.shutdown();
    _jmxReporter.close();
  }

  public String serverUrl() {
    return _server.getURI().toString();
  }

  private void printStartupInfo() {
    boolean corsEnabled = _config.getBoolean(WebServerConfig.WEBSERVER_HTTP_CORS_ENABLED_CONFIG);
    String webApiUrlPrefix = _config.getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG);
    String uiUrlPrefix = _config.getString(WebServerConfig.WEBSERVER_UI_URLPREFIX_CONFIG);
    String webDir = _config.getString(WebServerConfig.WEBSERVER_UI_DISKPATH_CONFIG);
    String sessionPath = _config.getString(WebServerConfig.WEBSERVER_SESSION_PATH_CONFIG);
    System.out.println(">> ********************************************* <<");
    System.out.println(">> Application directory            : " + System.getProperty("user.dir"));
    System.out.println(">> REST API available on            : " + webApiUrlPrefix);
    System.out.println(">> Web UI available on              : " + uiUrlPrefix);
    System.out.println(">> Web UI Directory                 : " + webDir);
    System.out.println(">> Cookie prefix path               : " + sessionPath);
    System.out.println(">> Kafka Cruise Control started on  : " + serverUrl());
    System.out.println(">> CORS Enabled ?                   : " + corsEnabled);
    System.out.println(">> ********************************************* <<");
  }

  private static KafkaCruiseControlConfig readConfig(String propertiesFile) throws IOException {
    Properties props = new Properties();
    try (InputStream propStream = new FileInputStream(propertiesFile)) {
      props.load(propStream);
    }

    return new KafkaCruiseControlConfig(props);
  }

  private void setupWebUi(ServletContextHandler contextHandler) {
    // Placeholder for any static content
    String webuiDir = _config.getString(WebServerConfig.WEBSERVER_UI_DISKPATH_CONFIG);
    String webuiPathPrefix = _config.getString(WebServerConfig.WEBSERVER_UI_URLPREFIX_CONFIG);
    DefaultServlet defaultServlet = new DefaultServlet();
    ServletHolder holderWebapp = new ServletHolder("default", defaultServlet);
    // holderWebapp.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
    holderWebapp.setInitParameter("resourceBase", webuiDir);
    contextHandler.addServlet(holderWebapp, webuiPathPrefix);
  }

  private NCSARequestLog createRequestLog() {
    boolean accessLogEnabled = _config.getBoolean(WebServerConfig.WEBSERVER_ACCESSLOG_ENABLED_CONFIG);
    if (accessLogEnabled) {
      String accessLogPath = _config.getString(WebServerConfig.WEBSERVER_ACCESSLOG_PATH_CONFIG);
      int accessLogRetention = _config.getInt(WebServerConfig.WEBSERVER_ACCESSLOG_RETENTION_DAYS_CONFIG);
      NCSARequestLog requestLog = new NCSARequestLog(accessLogPath);
      requestLog.setRetainDays(accessLogRetention);
      requestLog.setLogLatency(true);
      requestLog.setAppend(true);
      requestLog.setExtended(false);
      requestLog.setPreferProxiedForAddress(true);
      return requestLog;
    } else {
      return null;
    }
  }

  private ServletContextHandler createContextHandler() {
    // Define context for servlet
    String sessionPath = _config.getString(WebServerConfig.WEBSERVER_SESSION_PATH_CONFIG);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath(sessionPath);
    return context;
  }
}
