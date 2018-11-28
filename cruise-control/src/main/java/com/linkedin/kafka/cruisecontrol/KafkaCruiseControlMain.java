/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlet.DefaultServlet;
import java.net.InetSocketAddress;
import org.eclipse.jetty.server.NCSARequestLog;

/**
 * The main class to run Kafka Cruise Control.
 */
public class KafkaCruiseControlMain {
  private static final String METRIC_DOMAIN = "kafka.cruisecontrol";
  // Default API protocol exposed by the webserver
  private static final String DEFAULT_API_PROTOCOL = "http";

  private KafkaCruiseControlMain() {

  }

  public static void main(String[] args) throws Exception {
    // Check the command line arguments.
    if (args.length == 0) {
      printErrorMessageAndDie();
    }

    // Load all the properties.
    Properties props = new Properties();
    try (InputStream propStream = new FileInputStream(args[0])) {
      props.load(propStream);
    }

    // Get the configuration for Cruise Control
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    int port = config.getInt(KafkaCruiseControlConfig.WEBSERVER_HTTP_PORT_CONFIG);
    if (args.length > 1) {
      try {
        port = Integer.parseInt(args[1]);
      } catch (Exception e) {
        printErrorMessageAndDie();
      }
    }
    // Future ssl bits
    String protocol = DEFAULT_API_PROTOCOL;

    // For security reasons listen on the loopback address by default
    // Give preference to cmd-line bind address
    String hostname = config.getString(KafkaCruiseControlConfig.WEBSERVER_HTTP_ADDRESS_CONFIG);
    if (args.length > 2) {
      hostname = args[2];
    }

    MetricRegistry dropwizardMetricsRegistry = new MetricRegistry();
    JmxReporter jmxReporter = JmxReporter.forRegistry(dropwizardMetricsRegistry).inDomain(METRIC_DOMAIN).build();
    jmxReporter.start();

    AsyncKafkaCruiseControl kafkaCruiseControl = new AsyncKafkaCruiseControl(config,
                                                                             dropwizardMetricsRegistry);

    // Listen on a specific host & port combination
    Server server = new Server(new InetSocketAddress(hostname, port));

    // Setup Built-in Logger
    boolean accessLogEnabled = config.getBoolean(KafkaCruiseControlConfig.WEBSERVER_ACCESSLOG_ENABLED);
    if (accessLogEnabled) {
      String accessLogPath = config.getString(KafkaCruiseControlConfig.WEBSERVER_ACCESSLOG_PATH);
      int accessLogRetention = config.getInt(KafkaCruiseControlConfig.WEBSERVER_ACCESSLOG_RETENTION_DAYS);
      NCSARequestLog requestLog = new NCSARequestLog(accessLogPath);
      requestLog.setRetainDays(accessLogRetention);
      requestLog.setLogLatency(true);
      requestLog.setAppend(true);
      requestLog.setExtended(false);
      requestLog.setPreferProxiedForAddress(true);
      server.setRequestLog(requestLog);
    }

    // Define context for servlet
    String sessionPath = config.getString(KafkaCruiseControlConfig.WEBSERVER_SESSION_PATH);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath(sessionPath);
    server.setHandler(context);

    // Placeholder for any static content
    String webuiDir = config.getString(KafkaCruiseControlConfig.WEBSERVER_UI_DISKPATH);
    String webuiPathPrefix = config.getString(KafkaCruiseControlConfig.WEBSERVER_UI_URLPREFIX);
    DefaultServlet defaultServlet = new DefaultServlet();
    ServletHolder holderWebapp = new ServletHolder("default", defaultServlet);
    // holderWebapp.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
    holderWebapp.setInitParameter("resourceBase", webuiDir);
    context.addServlet(holderWebapp, webuiPathPrefix);

    // Kafka Cruise Control servlet data
    long maxBlockMs = config.getLong(KafkaCruiseControlConfig.WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS);
    long sessionExpiryMs = config.getLong(KafkaCruiseControlConfig.WEBSERVER_SESSION_EXPIRY_MS);
    String apiUrlPrefix = config.getString(KafkaCruiseControlConfig.WEBSERVER_API_URLPREFIX);
    KafkaCruiseControlServlet kafkaCruiseControlServlet =
        new KafkaCruiseControlServlet(kafkaCruiseControl, maxBlockMs, sessionExpiryMs, dropwizardMetricsRegistry, config);
    ServletHolder servletHolder = new ServletHolder(kafkaCruiseControlServlet);
    context.addServlet(servletHolder, apiUrlPrefix);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        kafkaCruiseControl.shutdown();
        jmxReporter.close();
      }
    });
    kafkaCruiseControl.startUp();
    server.start();
    boolean corsEnabled = config.getBoolean(KafkaCruiseControlConfig.WEBSERVER_HTTP_CORS_ENABLED_CONFIG);
    System.out.println(">> ********************************************* <<");
    System.out.println(">> Application directory            : " + System.getProperty("user.dir"));
    System.out.println(">> REST API available on            : " + apiUrlPrefix);
    System.out.println(">> Web UI available on              : " + webuiPathPrefix);
    System.out.println(">> Web UI Directory                 : " + webuiDir);
    System.out.println(">> Cookie prefix path               : " + sessionPath);
    System.out.println(">> Kafka Cruise Control started on  : " + protocol + "://" + hostname + ":" + port);
    System.out.println(">> CORS Enabled ?                   : " + corsEnabled);
    System.out.println(">> ********************************************* <<");
  }

  private static void printErrorMessageAndDie() {
    System.out.println(String.format("USAGE: java %s cruisecontrol.properties [port] [ipaddress|hostname]", KafkaCruiseControlMain.class.getSimpleName()));
    System.exit(-1);
  }
}
