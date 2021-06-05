/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class TopicMinIsrCacheTest {
  private static final Duration MOCK_CONCURRENCY_ADJUSTER_MIN_ISR_RETENTION_MS = Duration.ofMillis(2);
  private static final Duration MOCK_CONCURRENCY_ADJUSTER_EXPIRED_MIN_ISR_RETENTION_MS = Duration.ofMillis(-1);
  private static final int MOCK_CONCURRENCY_ADJUSTER_MIN_ISR_CACHE_SIZE = 3;
  private static final Duration MOCK_CACHE_CLEANER_PERIOD = Duration.ofMillis(200);
  private static final Duration MOCK_CACHE_CLEANER_FAST_PERIOD = Duration.ofMillis(10);
  private static final Duration MOCK_CACHE_CLEANER_INITIAL_DELAY = Duration.ofSeconds(0);
  private static final long MOCK_TIME_MS = 100L;
  private static final String MOCK_TOPIC = "mock-topic";
  private static final String MOCK_TOPIC_2 = "mock-topic-2";
  private static final String MOCK_TOPIC_3 = "mock-topic-3";
  private static final String MOCK_TOPIC_4 = "mock-topic-4";
  private static final ConfigResource MOCK_TOPIC_RESOURCE = new ConfigResource(ConfigResource.Type.TOPIC, MOCK_TOPIC);
  public static final String MIN_IN_SYNC_REPLICAS_VALUE = "2";
  private static final ConfigResource MOCK_TOPIC_RESOURCE_2 = new ConfigResource(ConfigResource.Type.TOPIC, MOCK_TOPIC_2);
  public static final String MIN_IN_SYNC_REPLICAS_VALUE_2 = "3";
  private static final ConfigResource MOCK_TOPIC_RESOURCE_3 = new ConfigResource(ConfigResource.Type.TOPIC, MOCK_TOPIC_3);
  public static final String MIN_IN_SYNC_REPLICAS_VALUE_3 = "4";
  private static final ConfigResource MOCK_TOPIC_RESOURCE_4 = new ConfigResource(ConfigResource.Type.TOPIC, MOCK_TOPIC_4);
  public static final String MIN_IN_SYNC_REPLICAS_VALUE_4 = "5";

  private Time _mockTime;

  /**
   * Setup the unit test.
   */
  @Before
  public void setup() {
    _mockTime = EasyMock.mock(Time.class);
  }

  @Test
  public void testCacheExpiration() throws ExecutionException, InterruptedException {
    EasyMock.expect(_mockTime.milliseconds()).andReturn(MOCK_TIME_MS).anyTimes();
    EasyMock.replay(_mockTime);

    // Emulate cache expiration with (1) a current time equals to updateTimeMs and (2) a negative retention time.
    TopicMinIsrCache topicMinIsrCache = new TopicMinIsrCache(MOCK_CONCURRENCY_ADJUSTER_EXPIRED_MIN_ISR_RETENTION_MS,
                                                             MOCK_CONCURRENCY_ADJUSTER_MIN_ISR_CACHE_SIZE,
                                                             MOCK_CACHE_CLEANER_FAST_PERIOD,
                                                             MOCK_CACHE_CLEANER_INITIAL_DELAY,
                                                             _mockTime);

    DescribeConfigsResult describeConfigsResult = EasyMock.mock(DescribeConfigsResult.class);

    List<KafkaFuture<Config>> describedConfigsFutures = new ArrayList<>(3);
    List<Config> topicConfigs = new ArrayList<>(3);
    for (int i = 0; i < 3; i++) {
      describedConfigsFutures.add(EasyMock.createMock(KafkaFuture.class));
      topicConfigs.add(EasyMock.createMock(Config.class));
    }

    Map<ConfigResource, KafkaFuture<Config>> describeConfigsValues = new LinkedHashMap<>(3);
    describeConfigsValues.put(MOCK_TOPIC_RESOURCE, describedConfigsFutures.get(0));
    describeConfigsValues.put(MOCK_TOPIC_RESOURCE_2, describedConfigsFutures.get(1));
    describeConfigsValues.put(MOCK_TOPIC_RESOURCE_3, describedConfigsFutures.get(2));

    EasyMock.expect(describeConfigsResult.values()).andReturn(describeConfigsValues);
    for (int i = 0; i < 3; i++) {
      EasyMock.expect(describedConfigsFutures.get(i).get()).andReturn(topicConfigs.get(i));
    }
    EasyMock.expect(topicConfigs.get(0).get(EasyMock.eq(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)))
            .andReturn(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_IN_SYNC_REPLICAS_VALUE));
    EasyMock.expect(topicConfigs.get(1).get(EasyMock.eq(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)))
            .andReturn(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_IN_SYNC_REPLICAS_VALUE_2));
    EasyMock.expect(topicConfigs.get(2).get(EasyMock.eq(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)))
            .andReturn(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_IN_SYNC_REPLICAS_VALUE_3));

    for (int i = 0; i < 3; i++) {
      EasyMock.replay(describedConfigsFutures.get(i), topicConfigs.get(i));
    }
    EasyMock.replay(describeConfigsResult);
    int sizeAfterPut = topicMinIsrCache.putTopicMinIsr(describeConfigsResult);
    assertEquals(3, sizeAfterPut);

    long startMs = System.currentTimeMillis();
    long deadlineMs = startMs + (10 * MOCK_CACHE_CLEANER_FAST_PERIOD.toMillis());
    long cacheWaitMs = MOCK_CACHE_CLEANER_FAST_PERIOD.toMillis() / 2;
    while (!topicMinIsrCache.minIsrWithTimeByTopic().isEmpty() && System.currentTimeMillis() < deadlineMs) {
      // wait for the cache to be cleaned.
      Thread.sleep(cacheWaitMs);
    }
    // The cache is expected to be empty upon cleanup.
    assertTrue(topicMinIsrCache.minIsrWithTimeByTopic().isEmpty());
    EasyMock.verify(describeConfigsResult);
  }

  @Test
  public void testPutTopicMinIsr() throws ExecutionException, InterruptedException {

    EasyMock.expect(_mockTime.milliseconds()).andReturn(MOCK_TIME_MS).anyTimes();
    EasyMock.replay(_mockTime);
    TopicMinIsrCache topicMinIsrCache = new TopicMinIsrCache(MOCK_CONCURRENCY_ADJUSTER_MIN_ISR_RETENTION_MS,
                                                             MOCK_CONCURRENCY_ADJUSTER_MIN_ISR_CACHE_SIZE,
                                                             MOCK_CACHE_CLEANER_PERIOD,
                                                             MOCK_CACHE_CLEANER_INITIAL_DELAY,
                                                             _mockTime);

    // 0. Test null describeConfigsResult input
    DescribeConfigsResult describeConfigsResult = null;
    topicMinIsrCache.putTopicMinIsr(describeConfigsResult);
    assertTrue(topicMinIsrCache.minIsrWithTimeByTopic().isEmpty());

    // 1. Test (1.1) Successful put, (1.2) Put with TimeoutException
    describeConfigsResult = EasyMock.mock(DescribeConfigsResult.class);
    KafkaFuture<Config> describedConfigsFuture = EasyMock.createMock(KafkaFuture.class);
    Config topicConfig = EasyMock.createMock(Config.class);

    Map<ConfigResource, KafkaFuture<Config>> describeConfigsValues = Collections.singletonMap(MOCK_TOPIC_RESOURCE,
                                                                                              describedConfigsFuture);
    EasyMock.expect(describeConfigsResult.values()).andReturn(describeConfigsValues).times(3);
    org.apache.kafka.common.errors.TimeoutException kafkaTimeoutException = new org.apache.kafka.common.errors.TimeoutException();
    EasyMock.expect(describedConfigsFuture.get()).andReturn(topicConfig).andThrow(new ExecutionException(kafkaTimeoutException));
    EasyMock.expect(topicConfig.get(EasyMock.eq(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)))
            .andReturn(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_IN_SYNC_REPLICAS_VALUE));

    EasyMock.replay(describeConfigsResult, describedConfigsFuture, topicConfig);

    // 1.1. Success -- config for MOCK_TOPIC is cached.
    // 1.2. TimeoutException -- config for MOCK_TOPIC is still cached.
    for (int i = 0; i < 2; i++) {
      topicMinIsrCache.putTopicMinIsr(describeConfigsResult);
      Map<String, TopicMinIsrCache.MinIsrWithTime> minIsrWithTimeByTopic = topicMinIsrCache.minIsrWithTimeByTopic();
      assertEquals(1, minIsrWithTimeByTopic.size());
      assertEquals(Integer.parseInt(MIN_IN_SYNC_REPLICAS_VALUE), minIsrWithTimeByTopic.get(MOCK_TOPIC).minISR());
      assertEquals(MOCK_TIME_MS, minIsrWithTimeByTopic.get(MOCK_TOPIC).timeMs());
    }

    EasyMock.verify(_mockTime, describeConfigsResult, describedConfigsFuture, topicConfig);

    // 2. Test put for 2 described topic configs and one described config having ExecutionException that is not due to TimeoutException.
    EasyMock.reset(describeConfigsResult, describedConfigsFuture);

    KafkaFuture<Config> describedConfigsFuture2 = EasyMock.createMock(KafkaFuture.class);
    Config topicConfig2 = EasyMock.createMock(Config.class);

    Map<ConfigResource, KafkaFuture<Config>> describeConfigsWithMultipleValues = new LinkedHashMap<>(2);
    describeConfigsWithMultipleValues.put(MOCK_TOPIC_RESOURCE_2, describedConfigsFuture2);
    describeConfigsWithMultipleValues.put(MOCK_TOPIC_RESOURCE_3, describedConfigsFuture);

    EasyMock.expect(describeConfigsResult.values()).andReturn(describeConfigsWithMultipleValues);
    EasyMock.expect(describedConfigsFuture.get()).andThrow(new ExecutionException(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception()));
    EasyMock.expect(describedConfigsFuture2.get()).andReturn(topicConfig2);
    EasyMock.expect(topicConfig2.get(EasyMock.eq(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)))
            .andReturn(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_IN_SYNC_REPLICAS_VALUE_2));

    EasyMock.replay(describeConfigsResult, describedConfigsFuture, describedConfigsFuture2, topicConfig2);
    topicMinIsrCache.putTopicMinIsr(describeConfigsResult);
    Map<String, TopicMinIsrCache.MinIsrWithTime> minIsrWithTimeByTopic = topicMinIsrCache.minIsrWithTimeByTopic();
    assertEquals(2, minIsrWithTimeByTopic.size());
    assertEquals(Integer.parseInt(MIN_IN_SYNC_REPLICAS_VALUE), minIsrWithTimeByTopic.get(MOCK_TOPIC).minISR());
    assertEquals(Integer.parseInt(MIN_IN_SYNC_REPLICAS_VALUE_2), minIsrWithTimeByTopic.get(MOCK_TOPIC_2).minISR());
    assertEquals(MOCK_TIME_MS, minIsrWithTimeByTopic.get(MOCK_TOPIC).timeMs());
    assertEquals(MOCK_TIME_MS, minIsrWithTimeByTopic.get(MOCK_TOPIC_2).timeMs());

    EasyMock.verify(_mockTime, describeConfigsResult, describedConfigsFuture, describedConfigsFuture2, topicConfig2);

    // 3. Test put for 2 described topic configs and one described config having InterruptedException.
    EasyMock.reset(describeConfigsResult, describedConfigsFuture);

    KafkaFuture<Config> describedConfigsFuture3 = EasyMock.createMock(KafkaFuture.class);
    Config topicConfig3 = EasyMock.createMock(Config.class);

    describeConfigsWithMultipleValues = new LinkedHashMap<>(2);
    describeConfigsWithMultipleValues.put(MOCK_TOPIC_RESOURCE_2, describedConfigsFuture);
    describeConfigsWithMultipleValues.put(MOCK_TOPIC_RESOURCE_3, describedConfigsFuture3);

    EasyMock.expect(describeConfigsResult.values()).andReturn(describeConfigsWithMultipleValues);
    EasyMock.expect(describedConfigsFuture.get()).andThrow(new InterruptedException());
    EasyMock.expect(describedConfigsFuture3.get()).andReturn(topicConfig3);
    EasyMock.expect(topicConfig3.get(EasyMock.eq(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)))
            .andReturn(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_IN_SYNC_REPLICAS_VALUE_3));

    EasyMock.replay(describeConfigsResult, describedConfigsFuture, describedConfigsFuture3, topicConfig3);
    topicMinIsrCache.putTopicMinIsr(describeConfigsResult);
    minIsrWithTimeByTopic = topicMinIsrCache.minIsrWithTimeByTopic();
    assertEquals(3, minIsrWithTimeByTopic.size());
    assertEquals(Integer.parseInt(MIN_IN_SYNC_REPLICAS_VALUE), minIsrWithTimeByTopic.get(MOCK_TOPIC).minISR());
    assertEquals(Integer.parseInt(MIN_IN_SYNC_REPLICAS_VALUE_2), minIsrWithTimeByTopic.get(MOCK_TOPIC_2).minISR());
    assertEquals(Integer.parseInt(MIN_IN_SYNC_REPLICAS_VALUE_3), minIsrWithTimeByTopic.get(MOCK_TOPIC_3).minISR());
    assertEquals(MOCK_TIME_MS, minIsrWithTimeByTopic.get(MOCK_TOPIC).timeMs());
    assertEquals(MOCK_TIME_MS, minIsrWithTimeByTopic.get(MOCK_TOPIC_2).timeMs());
    assertEquals(MOCK_TIME_MS, minIsrWithTimeByTopic.get(MOCK_TOPIC_3).timeMs());

    EasyMock.verify(_mockTime, describeConfigsResult, describedConfigsFuture, describedConfigsFuture3, topicConfig3);

    // 4. Test putting one more new entry when the cache is full.
    EasyMock.reset(describeConfigsResult);

    KafkaFuture<Config> describedConfigsFuture4 = EasyMock.createMock(KafkaFuture.class);
    Config topicConfig4 = EasyMock.createMock(Config.class);
    describeConfigsValues = Collections.singletonMap(MOCK_TOPIC_RESOURCE_4, describedConfigsFuture4);

    EasyMock.expect(describeConfigsResult.values()).andReturn(describeConfigsValues);
    EasyMock.expect(describedConfigsFuture4.get()).andReturn(topicConfig4);
    EasyMock.expect(topicConfig4.get(EasyMock.eq(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)))
            .andReturn(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_IN_SYNC_REPLICAS_VALUE_4));

    EasyMock.replay(describeConfigsResult, describedConfigsFuture4, topicConfig4);
    topicMinIsrCache.putTopicMinIsr(describeConfigsResult);
    minIsrWithTimeByTopic = topicMinIsrCache.minIsrWithTimeByTopic();
    assertEquals(3, minIsrWithTimeByTopic.size());
    assertNull(minIsrWithTimeByTopic.get(MOCK_TOPIC));
    assertEquals(Integer.parseInt(MIN_IN_SYNC_REPLICAS_VALUE_2), minIsrWithTimeByTopic.get(MOCK_TOPIC_2).minISR());
    assertEquals(Integer.parseInt(MIN_IN_SYNC_REPLICAS_VALUE_3), minIsrWithTimeByTopic.get(MOCK_TOPIC_3).minISR());
    assertEquals(Integer.parseInt(MIN_IN_SYNC_REPLICAS_VALUE_4), minIsrWithTimeByTopic.get(MOCK_TOPIC_4).minISR());
    assertEquals(MOCK_TIME_MS, minIsrWithTimeByTopic.get(MOCK_TOPIC_2).timeMs());
    assertEquals(MOCK_TIME_MS, minIsrWithTimeByTopic.get(MOCK_TOPIC_3).timeMs());
    assertEquals(MOCK_TIME_MS, minIsrWithTimeByTopic.get(MOCK_TOPIC_4).timeMs());

    EasyMock.verify(_mockTime, describeConfigsResult, describedConfigsFuture4, topicConfig4);
  }
}
