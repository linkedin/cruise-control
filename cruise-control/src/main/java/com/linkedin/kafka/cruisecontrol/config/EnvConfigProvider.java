/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.provider.ConfigProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class EnvConfigProvider implements ConfigProvider {

  private Map<String, String> _preConfiguredEnvironmentVariables;

  @Override
  public ConfigData get(String path) {
    assertNoPath(path);
    return new ConfigData(getenv());
  }

  @Override
  public ConfigData get(String path, Set<String> keys) {
    assertNoPath(path);
    Map<String, String> filtered = new HashMap<>(getenv());
    filtered.keySet().retainAll(keys);
    return new ConfigData(filtered);
  }

  @Override
  public void close() {
    _preConfiguredEnvironmentVariables.clear();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _preConfiguredEnvironmentVariables = configs.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, kv -> (String) kv.getValue()));
  }

  private static void assertNoPath(String path) {
    if (path != null && !path.isEmpty()) {
      throw new ConfigException("EnvConfigProvider does not support paths. Found: " + path);
    }
  }

  private Map<String, String> getenv() {
    if (_preConfiguredEnvironmentVariables == null) {
      return System.getenv();
    } else {
      return _preConfiguredEnvironmentVariables;
    }
  }
}
