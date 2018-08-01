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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;


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

  @FunctionalInterface
  public interface Task {
    void run() throws Exception;
  }
}