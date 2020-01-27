/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import java.util.Map;


public class KafkaCruiseControlConfigUtils {

  private KafkaCruiseControlConfigUtils() {

  }

  /**
   * @return Configured instance.
   */
  public static <T> T getConfiguredInstance(Class<?> c, Class<T> t, Map<String, Object> configs) {
    Object instance;
    try {
      instance = c.newInstance();
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Could not instantiate class " + c.getName(), e);
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Could not instantiate class " + c.getName() + " Does it have a public no-argument constructor?", e);
    } catch (NullPointerException e) {
      throw new IllegalArgumentException("Attempt to get configured instance of null configuration " + t.getName(), e);
    }

    if (!t.isInstance(instance)) {
      throw new IllegalArgumentException(c.getName() + " is not an instance of " + t.getName());
    }
    T o = t.cast(instance);
    if (o instanceof CruiseControlConfigurable) {
      ((CruiseControlConfigurable) o).configure(configs);
    }
    return o;
  }
}
