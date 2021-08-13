/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.NUM_BROKERS_TO_ADD;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PARTITION_COUNT;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.TOPIC_PARAM;


public class RightsizeParameters extends AbstractParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(NUM_BROKERS_TO_ADD);
    validParameterNames.add(PARTITION_COUNT);
    validParameterNames.add(TOPIC_PARAM);
    validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
  protected int _numBrokersToAdd;
  protected int _partitionCount;
  protected Pattern _topic;

  public RightsizeParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _numBrokersToAdd = ParameterUtils.numBrokersToAdd(_request);
    _partitionCount = ParameterUtils.partitionCount(_request);
    _topic = ParameterUtils.topic(_request);
  }

  public int numBrokersToAdd() {
    return _numBrokersToAdd;
  }

  public int partitionCount() {
    return _partitionCount;
  }

  public Pattern topic() {
    return _topic;
  }

  @Override
  public SortedSet<String> caseInsensitiveParameterNames() {
    return CASE_INSENSITIVE_PARAMETER_NAMES;
  }
}
