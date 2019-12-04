/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.servlet.http.HttpServletResponse;


/**
 * The annotation to denote the attached field will be used as the field key in the JSON response of some endpoints.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface JsonResponseField {

  /**
   * Whether the field is required in JSON response or not.
   * @return True if it is a required field; otherwise it is an optional field.
   */
  boolean required() default true;

  /**
   * The response status of response which should contain the field.
   * @return The associated response status.
   */
  int responseStatus() default HttpServletResponse.SC_OK;

}

