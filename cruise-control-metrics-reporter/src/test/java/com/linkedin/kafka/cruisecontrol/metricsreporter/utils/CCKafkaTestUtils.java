/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;


public class CCKafkaTestUtils {
  private final static AtomicBoolean SHUTDOWN_HOOK_INSTALLED = new AtomicBoolean(false);
  private final static Thread SHUTDOWN_HOOK;
  private final static List<File> FILES_TO_CLEAN_UP = Collections.synchronizedList(new ArrayList<>());

  static {
    SHUTDOWN_HOOK = new Thread(() -> {
      Exception firstIssue = null;
      for (File toCleanUp : FILES_TO_CLEAN_UP) {
        if (!toCleanUp.exists()) {
          continue;
        }
        try {
          FileUtils.forceDelete(toCleanUp);
        } catch (IOException issue) {
          if (firstIssue == null) {
            firstIssue = issue;
          } else {
            firstIssue.addSuppressed(issue);
          }
        }
      }
      if (firstIssue != null) {
        System.err.println("unable to delete one or more files");
        firstIssue.printStackTrace(System.err);
        throw new IllegalStateException(firstIssue);
      }
    }, "CCKafkaTestUtils cleanup hook");
    SHUTDOWN_HOOK.setUncaughtExceptionHandler((t, e) -> {
      System.err.println("thread " + t.getName() + " died to uncaught exception");
      e.printStackTrace(System.err);
    });
  }

  private CCKafkaTestUtils() {
    //utility class
  }

  public static KafkaProducer<String, String> vanillaProducerFor(CCEmbeddedBroker broker) {
    String bootstrap = broker.getPlaintextAddr();
    if (bootstrap == null) {
      bootstrap = broker.getSslAddr();
    }

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrap);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 1024 * 1024);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return new KafkaProducer<>(props);
  }

  public static KafkaConsumer<String, String> vanillaConsumerFor(CCEmbeddedBroker broker) {
    String bootstrap = broker.getPlaintextAddr();
    if (bootstrap == null) {
      bootstrap = broker.getSslAddr();
    }

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrap);
    props.put("group.id", "test");
    props.put("auto.offset.reset", "earliest");
    props.put("enable.auto.commit", "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    return consumer;
  }

  public static File newTempDir() {
    try {
      return cleanup(Files.createTempDirectory(null).toFile());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static File cleanup(File toCleanUp) {
    if (SHUTDOWN_HOOK_INSTALLED.compareAndSet(false, true)) {
      Runtime.getRuntime().addShutdownHook(SHUTDOWN_HOOK);
    }
    FILES_TO_CLEAN_UP.add(toCleanUp);
    return toCleanUp;
  }

  public static void quietly(Task task) {
    try {
      task.run();
    } catch (Exception e) {
      e.printStackTrace(System.err);
    }
  }

  public static String getRandomString(int length) {
    char[] chars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    Random random = new Random();
    StringBuilder stringBuiler = new StringBuilder();
    for (int i = 0; i < length; i++) {
      stringBuiler.append(chars[Math.abs(random.nextInt()) % 16]);
    }
    return stringBuiler.toString();
  }

  @FunctionalInterface
  public interface Task {
    void run() throws Exception;
  }
}