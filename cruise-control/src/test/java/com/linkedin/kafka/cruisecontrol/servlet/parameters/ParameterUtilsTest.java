/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

  @Test
  public void testParseTimeRangeSet() {
    HttpServletRequest mockRequest = EasyMock.mock(HttpServletRequest.class);
    String startTimeString = "12345";
    String endTimeString = "23456";
    Map<String, String[]> paramMap = new HashMap<>();
    paramMap.put(ParameterUtils.START_MS_PARAM, new String[]{ParameterUtils.START_MS_PARAM});
    paramMap.put(ParameterUtils.END_MS_PARAM, new String[]{ParameterUtils.END_MS_PARAM});

    EasyMock.expect(mockRequest.getParameterMap()).andReturn(paramMap).anyTimes();
    EasyMock.expect(mockRequest.getParameter(ParameterUtils.START_MS_PARAM)).andReturn(startTimeString).anyTimes();
    EasyMock.expect(mockRequest.getParameter(ParameterUtils.END_MS_PARAM)).andReturn(endTimeString).anyTimes();
    EasyMock.replay(mockRequest);

    Long startMs = ParameterUtils.startMs(mockRequest);
    Long endMs = ParameterUtils.endMs(mockRequest);
    Assert.assertNotNull(startMs);
    Assert.assertNotNull(endMs);
    Assert.assertEquals((Long) Long.parseLong(startTimeString), startMs);
    Assert.assertEquals((Long) Long.parseLong(endTimeString), endMs);
    ParameterUtils.validateTimeRange(startMs, endMs);

    startMs = ParameterUtils.startMsOrDefault(mockRequest, DEFAULT_START_TIME_MS);
    endMs = ParameterUtils.endMsOrDefault(mockRequest, DEFAULT_END_TIME_MS);

    Assert.assertEquals((Long) Long.parseLong(startTimeString), startMs);
    Assert.assertEquals((Long) Long.parseLong(endTimeString), endMs);
  }

  @Test
  public void testParseTimeRangeNotSet() {
    HttpServletRequest mockRequest = EasyMock.mock(HttpServletRequest.class);
    // Mock the request so that it does not contain start/end time
    EasyMock.expect(mockRequest.getParameterMap()).andReturn(Collections.emptyMap()).anyTimes();
    EasyMock.replay(mockRequest);

    Long startMs = ParameterUtils.startMs(mockRequest);
    Long endMs = ParameterUtils.endMs(mockRequest);
    Assert.assertNull(startMs);
    Assert.assertNull(endMs);

    startMs = ParameterUtils.startMsOrDefault(mockRequest, DEFAULT_START_TIME_MS);
    endMs = ParameterUtils.endMsOrDefault(mockRequest, DEFAULT_END_TIME_MS);

    Assert.assertEquals(DEFAULT_START_TIME_MS, startMs);
    Assert.assertEquals(DEFAULT_END_TIME_MS, endMs);
  }
}
