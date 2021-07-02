/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public final class Utils {

  private Utils() {

  }

  /**
   * Get topic names that match with a given topic naming pattern
   * @param topicNamePattern target topic naming pattern
   * @param topicNamesSupplier a supplier that provides topic names
   * @return topic names
   */
  public static Set<String> getTopicNamesMatchedWithPattern(Pattern topicNamePattern, Supplier<Set<String>> topicNamesSupplier) {
    if (topicNamePattern.pattern().isEmpty()) {
      return Collections.emptySet();
    }
    return topicNamesSupplier.get()
                             .stream()
                             .filter(topicName -> topicNamePattern.matcher(topicName).matches())
                             .collect(Collectors.toSet());
  }
}
