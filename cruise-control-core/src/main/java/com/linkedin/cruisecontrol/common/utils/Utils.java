/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.common.utils;

import com.linkedin.cruisecontrol.exception.CruiseControlException;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;


public final class Utils {

  private Utils() {

  }

  /**
   * @param c Class for which a new instance will be instantiated.
   * @param <T> The type of the instance to be returned.
   * @return Instantiated class.
   */
  public static <T> T newInstance(Class<T> c) throws CruiseControlException {
    if (c == null) {
      throw new CruiseControlException("class cannot be null");
    }
    try {
      return c.getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException e) {
      throw new CruiseControlException("Could not find a public no-argument constructor for " + c.getName(), e);
    } catch (ReflectiveOperationException | RuntimeException e) {
      throw new CruiseControlException("Could not instantiate class " + c.getName(), e);
    }
  }

  /**
   * Look up the class by name and instantiate it.
   * @param klass class name
   * @param base super class of the class to be instantiated
   * @param <T> The instance type.
   * @return The new instance
   */
  public static <T> T newInstance(String klass, Class<T> base) throws ClassNotFoundException, CruiseControlException {
    return Utils.newInstance(Class.forName(klass, true, Utils.getContextOrCruiseControlClassLoader()).asSubclass(base));
  }

  /**
   * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that loaded Cruise Control.
   *
   * This should be used whenever passing a ClassLoader to Class.forName
   * @return the Context ClassLoader on this thread or, if not present, the ClassLoader that loaded Cruise Control.
   */
  public static ClassLoader getContextOrCruiseControlClassLoader() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      return getCruiseControlClassLoader();
    } else {
      return cl;
    }
  }

  /**
   * @return the ClassLoader which loaded Kafka.
   */
  public static ClassLoader getCruiseControlClassLoader() {
    return Utils.class.getClassLoader();
  }

  /**
   * Create a string representation of a list joined by the given separator
   * @param list The list of items
   * @param separator The separator
   * @param <T> The type of the items in the given list.
   * @return The string representation.
   */
  public static <T> String join(Collection<T> list, String separator) {
    StringBuilder sb = new StringBuilder();
    Iterator<T> iter = list.iterator();
    while (iter.hasNext()) {
      sb.append(iter.next());
      if (iter.hasNext()) {
        sb.append(separator);
      }
    }
    return sb.toString();
  }

  /**
   * Checks that the specified object reference is not null and throws a customized IllegalArgumentException if it is.
   *
   * @param obj the object reference to check for nullity
   * @param errorMsg message to be used in the event that a IllegalArgumentException is thrown
   * @param <T> the type of the reference
   * @return obj if not null
   * @throws IllegalArgumentException if obj is null
   */
  public static <T> T validateNotNull(T obj, String errorMsg) {
    if (obj == null) {
      throw new IllegalArgumentException(errorMsg);
    }
    return obj;
   }

  /**
   * Checks that the specified object reference is not null and throws a customized IllegalArgumentException if it is.
   *
   * @param obj the object reference to check for nullity
   * @param errorMsgSupplier supplier of the message to be used in the event that a IllegalArgumentException is thrown
   * @param <T> the type of the reference
   * @return obj if not null
   * @throws IllegalArgumentException if obj is null
   */
  public static <T> T validateNotNull(T obj, Supplier<String> errorMsgSupplier) {
    if (obj == null) {
      throw new IllegalArgumentException(errorMsgSupplier.get());
    }
    return obj;
  }
}
