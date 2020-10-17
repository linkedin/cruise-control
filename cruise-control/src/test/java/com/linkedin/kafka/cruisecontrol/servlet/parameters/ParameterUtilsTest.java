/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;


public class ParameterUtilsTest {

  private static final Long DEFAULT_START_TIME_MS = -1L;
  private static final Long DEFAULT_END_TIME_MS = -2L;
  private static final String START_TIME_STRING = "12345";
  private static final String END_TIME_STRING = "23456";

  @Test
  public void testParseTimeRangeSet() {
    HttpServletRequest mockRequest = EasyMock.mock(HttpServletRequest.class);
    Map<String, String[]> paramMap = new HashMap<>();
    paramMap.put(ParameterUtils.START_MS_PARAM, new String[]{ParameterUtils.START_MS_PARAM});
    paramMap.put(ParameterUtils.END_MS_PARAM, new String[]{ParameterUtils.END_MS_PARAM});

    EasyMock.expect(mockRequest.getParameterMap()).andReturn(paramMap).times(4);
    EasyMock.expect(mockRequest.getParameter(ParameterUtils.START_MS_PARAM)).andReturn(START_TIME_STRING).times(2);
    EasyMock.expect(mockRequest.getParameter(ParameterUtils.END_MS_PARAM)).andReturn(END_TIME_STRING).times(2);
    EasyMock.replay(mockRequest);

    Long startMs = ParameterUtils.startMsOrDefault(mockRequest, null);
    Long endMs = ParameterUtils.endMsOrDefault(mockRequest, null);
    Assert.assertNotNull(startMs);
    Assert.assertNotNull(endMs);
    Assert.assertEquals((Long) Long.parseLong(START_TIME_STRING), startMs);
    Assert.assertEquals((Long) Long.parseLong(END_TIME_STRING), endMs);
    ParameterUtils.validateTimeRange(startMs, endMs);

    startMs = ParameterUtils.startMsOrDefault(mockRequest, DEFAULT_START_TIME_MS);
    endMs = ParameterUtils.endMsOrDefault(mockRequest, DEFAULT_END_TIME_MS);

    Assert.assertEquals(Long.parseLong(START_TIME_STRING), startMs.longValue());
    Assert.assertEquals(Long.parseLong(END_TIME_STRING), endMs.longValue());
  }

  @Test
  public void testParseTimeRangeNotSet() {
    HttpServletRequest mockRequest = EasyMock.mock(HttpServletRequest.class);
    // Mock the request so that it does not contain start/end time
    EasyMock.expect(mockRequest.getParameterMap()).andReturn(Collections.emptyMap()).times(4);
    EasyMock.replay(mockRequest);

    Long startMs = ParameterUtils.startMsOrDefault(mockRequest, null);
    Long endMs = ParameterUtils.endMsOrDefault(mockRequest, null);
    Assert.assertNull(startMs);
    Assert.assertNull(endMs);

    startMs = ParameterUtils.startMsOrDefault(mockRequest, DEFAULT_START_TIME_MS);
    endMs = ParameterUtils.endMsOrDefault(mockRequest, DEFAULT_END_TIME_MS);

    Assert.assertEquals(DEFAULT_START_TIME_MS, startMs);
    Assert.assertEquals(DEFAULT_END_TIME_MS, endMs);
  }
}
