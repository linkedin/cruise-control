/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;


/**
 * The main class to run Kafka Cruise Control.
 */
public class KafkaCruiseControlMain {

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

    KafkaCruiseControl kafkaCruiseControl = new KafkaCruiseControl(new KafkaCruiseControlConfig(props));

    Server server = new Server(port);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);
    KafkaCruiseControlServlet kafkaCruiseControlServlet = new KafkaCruiseControlServlet(kafkaCruiseControl);
    ServletHolder servletHolder = new ServletHolder(kafkaCruiseControlServlet);
    context.addServlet(servletHolder, "/kafkacruisecontrol/*");
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        kafkaCruiseControl.shutdown();
      }
    });
    kafkaCruiseControl.startUp();
    server.start();
    System.out.println("Kafka Cruise Control started.");
  }

  private static void printErrorMessageAndDie() {
    System.out.println(String.format("USAGE: java %s cruisecontrol.properties [port]", KafkaCruiseControlMain.class.getSimpleName()));
    System.exit(-1);
  }
}
