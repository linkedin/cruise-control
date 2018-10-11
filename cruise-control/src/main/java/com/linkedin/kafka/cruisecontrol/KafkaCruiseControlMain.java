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

    int port = 9090;
    if (args.length > 1) {
      try {
        port = Integer.parseInt(args[1]);
      } catch (Exception e) {
        printErrorMessageAndDie();
      }
    }
    // Future ssl bits
    String protocol = "http";

    // For security reasons listen on the loopback address by default
    String hostname = "127.0.0.1";
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
    holderWebapp.setInitParameter("resourceBase", "./cruise-control-ui/dist/");
    context.addServlet(holderWebapp, "/*");
    // Kafka Cruise Control servlet data
    KafkaCruiseControlServlet kafkaCruiseControlServlet =
        new KafkaCruiseControlServlet(kafkaCruiseControl, 10000L, 60000L, dropwizardMetricsRegistry);
    ServletHolder servletHolder = new ServletHolder(kafkaCruiseControlServlet);
    context.addServlet(servletHolder, "/kafkacruisecontrol/*");
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
