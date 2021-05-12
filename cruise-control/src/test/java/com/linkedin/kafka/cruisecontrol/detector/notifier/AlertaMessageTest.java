/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import org.junit.Test;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class AlertaMessageTest {

  @Test
  public void testAlertaMessageJsonFormat() {
    AlertaMessage alertaMessage = new AlertaMessage("resource", "event");
    alertaMessage.setCorrelate(Arrays.asList("cor1", "cor2"));
    alertaMessage.setCreateTime("2021-03-23 10:00:00.000Z");
    alertaMessage.setEnvironment("env");
    alertaMessage.setGroup("group");
    alertaMessage.setOrigin("origin");
    alertaMessage.setRawData("raw");
    alertaMessage.setService(Arrays.asList("service1", "service2"));
    alertaMessage.setSeverity("severity");
    alertaMessage.setTags(Arrays.asList("tag1", "tag2"));
    alertaMessage.setText("text");
    alertaMessage.setTimeout("300");
    alertaMessage.setType("type");
    alertaMessage.setValue("value");

    String expectedJson =
        "{\"correlate\":[\"cor1\",\"cor2\"],"
        + "\"createTime\":\"2021-03-23 10:00:00.000Z\","
        + "\"environment\":\"env\","
        + "\"event\":\"event\","
        + "\"group\":\"group\","
        + "\"origin\":\"origin\","
        + "\"rawData\":\"raw\","
        + "\"resource\":\"resource\","
        + "\"service\":[\"service1\",\"service2\"],"
        + "\"severity\":\"severity\","
        + "\"tags\":[\"tag1\",\"tag2\"],"
        + "\"text\":\"text\","
        + "\"timeout\":\"300\","
        + "\"type\":\"type\","
        + "\"value\":\"value\"}";

    assertEquals(expectedJson, alertaMessage.toString());
  }

  @Test
  public void testAlertaMessageJsonFormatWithoutNullValues() {
    AlertaMessage alertaMessage = new AlertaMessage("resource", "event");

    String expectedJson =
        "{\"event\":\"event\","
        + "\"resource\":\"resource\"}";

    assertEquals(expectedJson, alertaMessage.toString());
  }
}
