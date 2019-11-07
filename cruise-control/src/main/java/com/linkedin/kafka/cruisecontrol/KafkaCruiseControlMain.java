/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import java.io.IOException;

/**
 * The main class to run Kafka Cruise Control.
 */
public class KafkaCruiseControlMain {

  private KafkaCruiseControlMain() { }

  /**
   * The main function to run Cruise Control.
   * @param args Arguments passed while starting Cruise Control.
   */
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      printErrorMessageAndDie();
    }
    try {
      Integer port = parsePort(args);
      String hostname = parseHostname(args);
      KafkaCruiseControlApp app = new KafkaCruiseControlApp(args[0], port, hostname);
      app.registerShutdownHook();
      app.start();
    } catch (IOException | IllegalArgumentException e) {
      printErrorMessageAndDie();
    }
  }

  private static Integer parsePort(String[] args) {
    try {
      if (args.length > 1) {
        return Integer.parseInt(args[1]);
      }
    } catch (Exception e) {
      printErrorMessageAndDie();
    }
    return null;
  }

  private static String parseHostname(String[] args) {
    try {
      if (args.length > 2) {
        return args[2];
      }
    } catch (Exception e) {
      printErrorMessageAndDie();
    }
    return null;
  }

  private static void printErrorMessageAndDie() {
    System.out.println(String.format("USAGE: java %s cruisecontrol.properties [port] [ipaddress|hostname]", KafkaCruiseControlMain.class.getSimpleName()));
    System.exit(-1);
  }
}
