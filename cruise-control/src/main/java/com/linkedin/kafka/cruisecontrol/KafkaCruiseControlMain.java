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

/**
 * The main class to run Kafka Cruise Control.
 */
public class KafkaCruiseControlMain {
  private static final String METRIC_DOMAIN = "kafka.cruisecontrol";
  // Default API protocol exposed by the webserver
  private static final String DEFAULT_API_PROTOCOL = "http";
  // IP Address on which the service is made available by default
  private static final String DEFAULT_BIND_ADDRESS = "127.0.0.1";
  // Port Number on which the service listens by default
  private static final int DEFAULT_BIND_PORT = 9090;
  // API Prefix (from server root) under which all resources are made available
  private static final String DEFAULT_API_PATH_PREFIX = "/kafkacruisecontrol/*";
  // Place where the Admin UI contents are made available
  private static final String DEFAULT_WEBUI_DIR = "./cruise-control-ui/dist/";
  // Default URL for Admin UI
  private static final String DEFAULT_WEBUI_PATH_PREFIX = "/*";

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

    int port = DEFAULT_BIND_PORT;
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
    String hostname = DEFAULT_BIND_ADDRESS;
    if (args.length > 2) {
      hostname = args[2];
    }

    MetricRegistry dropwizardMetricsRegistry = new MetricRegistry();
    JmxReporter jmxReporter = JmxReporter.forRegistry(dropwizardMetricsRegistry).inDomain(METRIC_DOMAIN).build();
    jmxReporter.start();

    AsyncKafkaCruiseControl kafkaCruiseControl = new AsyncKafkaCruiseControl(new KafkaCruiseControlConfig(props),
                                                                             dropwizardMetricsRegistry);

    // Listen on a specific host & port combination
    Server server = new Server(new InetSocketAddress(hostname, port));

    // Define context for servlet
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);

    // Placeholder for any static content
    DefaultServlet defaultServlet = new DefaultServlet();
    ServletHolder holderWebapp = new ServletHolder("default", defaultServlet);
    holderWebapp.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
    holderWebapp.setInitParameter("resourceBase", DEFAULT_WEBUI_DIR);
    context.addServlet(holderWebapp, DEFAULT_WEBUI_PATH_PREFIX);

    // Kafka Cruise Control servlet data
    KafkaCruiseControlServlet kafkaCruiseControlServlet =
        new KafkaCruiseControlServlet(kafkaCruiseControl, 10000L, 60000L, dropwizardMetricsRegistry);
    ServletHolder servletHolder = new ServletHolder(kafkaCruiseControlServlet);
    context.addServlet(servletHolder, DEFAULT_API_PATH_PREFIX);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        kafkaCruiseControl.shutdown();
        jmxReporter.close();
      }
    });
    kafkaCruiseControl.startUp();
    server.start();
    System.out.println("Application directory: " + System.getProperty("user.dir"));
    System.out.println("Kafka Cruise Control started on " + protocol + "://" + hostname + ":" + port);
  }

  private static void printErrorMessageAndDie() {
    System.out.println(String.format("USAGE: java %s cruisecontrol.properties [port] [ipaddress|hostname]", KafkaCruiseControlMain.class.getSimpleName()));
    System.exit(-1);
  }
}
