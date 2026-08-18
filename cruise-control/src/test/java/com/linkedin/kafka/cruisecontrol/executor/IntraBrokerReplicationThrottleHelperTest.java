/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.easymock.EasyMock;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.EXECUTION_ALERTING_THRESHOLD_MS;
import static com.linkedin.kafka.cruisecontrol.executor.IntraBrokerReplicationThrottleHelper.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class IntraBrokerReplicationThrottleHelperTest {

  private static final Config EMPTY_CONFIG = new Config(Collections.emptyList());

  @Test
  public void testIsNoOpWhenThrottleIsNull() throws Exception {
    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    EasyMock.replay(mockAdminClient);

    // If throttle is null, no admin client interactions should happen
    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, null);
    ExecutionTask task = createIntraBrokerTask(0, 1);
    task.inProgress(0);
    task.completed(1);

    helper.setThrottles(Collections.singletonList(task));
    helper.clearThrottles(Collections.singletonList(task), Collections.emptyList());
    helper.clearAllThrottles();
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testSetThrottles() throws Exception {
    final long throttleRate = 1000000L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);

    // Expect describeConfigs for each broker (returns empty config - no existing throttle)
    expectDescribeBrokerConfigs(mockAdminClient, brokerId0, EMPTY_CONFIG);
    expectDescribeBrokerConfigs(mockAdminClient, brokerId1, EMPTY_CONFIG);

    // Expect incrementalAlterConfigs to set the throttle on each broker
    expectIncrementalAlterBrokerConfigs(mockAdminClient, brokerId0);
    expectIncrementalAlterBrokerConfigs(mockAdminClient, brokerId1);

    // Expect describeConfigs again for waitForConfigs verification
    Config configAfterSet = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, String.valueOf(throttleRate))));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId0, configAfterSet);
    expectDescribeBrokerConfigs(mockAdminClient, brokerId1, configAfterSet);

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    ExecutionTask task0 = createIntraBrokerTask(0, brokerId0);
    ExecutionTask task1 = createIntraBrokerTask(1, brokerId1);

    helper.setThrottles(Arrays.asList(task0, task1));
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testSetThrottleSkipsWhenAlreadySet() throws Exception {
    final long throttleRate = 1000000L;
    final int brokerId = 0;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);

    // Broker already has the correct throttle rate set
    Config existingConfig = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, String.valueOf(throttleRate))));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId, existingConfig);
    // No incrementalAlterConfigs expected since throttle is already set

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    ExecutionTask task = createIntraBrokerTask(0, brokerId);
    helper.setThrottles(Collections.singletonList(task));
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testClearThrottlesForCompletedTasks() throws Exception {
    final long throttleRate = 1000000L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);

    // Broker 0 has dynamic throttle config to be removed
    Config dynamicThrottleConfig = new Config(Collections.singletonList(
        mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
            String.valueOf(throttleRate), ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId0, dynamicThrottleConfig);
    expectIncrementalAlterBrokerConfigs(mockAdminClient, brokerId0);
    // waitForConfigs - return empty after delete
    expectDescribeBrokerConfigs(mockAdminClient, brokerId0, EMPTY_CONFIG);

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    // Task on broker 0 is completed, task on broker 1 is still in progress
    ExecutionTask completedTask = createIntraBrokerTask(0, brokerId0);
    completedTask.inProgress(0);
    completedTask.completed(1);

    ExecutionTask inProgressTask = createIntraBrokerTask(1, brokerId1);
    inProgressTask.inProgress(0);

    helper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testClearThrottlesSkipsStaticConfig() throws Exception {
    final long throttleRate = 1000000L;
    final int brokerId = 0;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);

    // Broker has static throttle config - should not be removed
    Config staticThrottleConfig = new Config(Collections.singletonList(
        mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
            String.valueOf(throttleRate), ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId, staticThrottleConfig);
    // No incrementalAlterConfigs expected since it's a static config

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    ExecutionTask completedTask = createIntraBrokerTask(0, brokerId);
    completedTask.inProgress(0);
    completedTask.completed(1);

    helper.clearThrottles(Collections.singletonList(completedTask), Collections.emptyList());
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testClearAllThrottles() throws Exception {
    final long throttleRate = 1000000L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);

    // setThrottles: describeConfigs + alter + waitForConfigs for each broker
    expectDescribeBrokerConfigs(mockAdminClient, brokerId0, EMPTY_CONFIG);
    expectIncrementalAlterBrokerConfigs(mockAdminClient, brokerId0);
    Config configAfterSet = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, String.valueOf(throttleRate))));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId0, configAfterSet);

    expectDescribeBrokerConfigs(mockAdminClient, brokerId1, EMPTY_CONFIG);
    expectIncrementalAlterBrokerConfigs(mockAdminClient, brokerId1);
    expectDescribeBrokerConfigs(mockAdminClient, brokerId1, configAfterSet);

    // clearAllThrottles: describeConfigs + alter + waitForConfigs for each broker
    Config dynamicConfig = new Config(Collections.singletonList(
        mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
            String.valueOf(throttleRate), ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId0, dynamicConfig);
    expectIncrementalAlterBrokerConfigs(mockAdminClient, brokerId0);
    expectDescribeBrokerConfigs(mockAdminClient, brokerId0, EMPTY_CONFIG);

    expectDescribeBrokerConfigs(mockAdminClient, brokerId1, dynamicConfig);
    expectIncrementalAlterBrokerConfigs(mockAdminClient, brokerId1);
    expectDescribeBrokerConfigs(mockAdminClient, brokerId1, EMPTY_CONFIG);

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    ExecutionTask task0 = createIntraBrokerTask(0, brokerId0);
    ExecutionTask task1 = createIntraBrokerTask(1, brokerId1);

    helper.setThrottles(Arrays.asList(task0, task1));
    helper.clearAllThrottles();
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testDoNotRemoveThrottleForBrokerWithInProgressTask() throws Exception {
    final long throttleRate = 1000000L;
    final int brokerId = 0;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    // No interactions expected since broker 0 has both completed and in-progress tasks
    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    // Both tasks are on the same broker
    ExecutionTask completedTask = createIntraBrokerTask(0, brokerId);
    completedTask.inProgress(0);
    completedTask.completed(1);

    ExecutionTask inProgressTask = createIntraBrokerTask(1, brokerId);
    inProgressTask.inProgress(0);

    // Since broker 0 still has an in-progress task, throttle should not be removed
    helper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testWaitForConfigsThrowsOnTimeout() {
    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    int retries = 2;

    // Return empty config repeatedly (never matches expected), triggering timeout
    for (int i = 0; i <= retries; i++) {
      expectDescribeBrokerConfigs(mockAdminClient, 0, EMPTY_CONFIG);
    }

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, 100L, retries);
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, "0");
    assertThrows(IllegalStateException.class, () -> helper.waitForConfigs(cf, Collections.singletonList(
        new AlterConfigOp(new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "100"),
            AlterConfigOp.OpType.SET))));
  }

  @Test
  public void testConfigsEqual() {
    Map<String, String> expectedConfigs = new HashMap<>();

    assertTrue(IntraBrokerReplicationThrottleHelper.configsEqual(EMPTY_CONFIG, expectedConfigs));

    expectedConfigs.put(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "1000000");
    assertFalse(IntraBrokerReplicationThrottleHelper.configsEqual(EMPTY_CONFIG, expectedConfigs));

    Config matchingConfig = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "1000000")));
    assertTrue(IntraBrokerReplicationThrottleHelper.configsEqual(matchingConfig, expectedConfigs));

    Config mismatchConfig = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "500000")));
    assertFalse(IntraBrokerReplicationThrottleHelper.configsEqual(mismatchConfig, expectedConfigs));

    // Null expected value with empty config value should be treated as equal
    Map<String, String> nullExpected = new HashMap<>();
    nullExpected.put(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, null);
    assertTrue(IntraBrokerReplicationThrottleHelper.configsEqual(EMPTY_CONFIG, nullExpected));

    // Static broker config with null expected should be treated as cleared
    ConfigEntry mockStaticEntry = mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
        "1000000", ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG);
    Config staticConfig = new Config(Collections.singletonList(mockStaticEntry));
    assertTrue(IntraBrokerReplicationThrottleHelper.configsEqual(staticConfig, nullExpected));
    EasyMock.verify(mockStaticEntry);

    // Dynamic default broker config with null expected should be treated as cleared (inherited value exposed after DELETE)
    ConfigEntry mockDefaultEntry = mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
        "500000", ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG);
    Config defaultConfig = new Config(Collections.singletonList(mockDefaultEntry));
    assertTrue(IntraBrokerReplicationThrottleHelper.configsEqual(defaultConfig, nullExpected));
    EasyMock.verify(mockDefaultEntry);

    // Dynamic broker-specific config with null expected should NOT be treated as cleared (override still exists)
    ConfigEntry mockDynamicBrokerEntry = mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
        "750000", ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);
    Config dynamicBrokerConfig = new Config(Collections.singletonList(mockDynamicBrokerEntry));
    assertFalse(IntraBrokerReplicationThrottleHelper.configsEqual(dynamicBrokerConfig, nullExpected));
    EasyMock.verify(mockDynamicBrokerEntry);
  }

  @Test
  public void testRestoresPreExistingThrottleOnCleanup() throws Exception {
    final long throttleRate = 2000000L;
    final String preExistingRate = "500000";
    final int brokerId = 0;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);

    // setThrottles: describeConfigs returns pre-existing dynamic broker config
    Config preExistingConfig = new Config(Collections.singletonList(
        mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
            preExistingRate, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId, preExistingConfig);
    // Should overwrite with our throttle rate
    expectIncrementalAlterBrokerConfigs(mockAdminClient, brokerId);
    // waitForConfigs verification
    Config configAfterSet = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, String.valueOf(throttleRate))));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId, configAfterSet);

    // clearAllThrottles: describeConfigs returns our throttle (dynamic broker config)
    Config ourConfig = new Config(Collections.singletonList(
        mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
            String.valueOf(throttleRate), ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId, ourConfig);
    // Should restore the original value (SET, not DELETE)
    expectIncrementalAlterBrokerConfigs(mockAdminClient, brokerId);
    // waitForConfigs verification - returns the restored value
    Config restoredConfig = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, preExistingRate)));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId, restoredConfig);

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    ExecutionTask task = createIntraBrokerTask(0, brokerId);
    helper.setThrottles(Collections.singletonList(task));
    helper.clearAllThrottles();
    EasyMock.verify(mockAdminClient);
  }

  // --- Helper methods ---

  private ExecutionTask createIntraBrokerTask(long executionId, int brokerId) {
    ExecutionProposal proposal = new ExecutionProposal(
        new TopicPartition("test-topic", 0),
        100,
        new com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo(brokerId),
        Collections.singletonList(new com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo(brokerId)),
        Collections.singletonList(new com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo(brokerId)));
    return new ExecutionTask(executionId, proposal, brokerId, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION,
        EXECUTION_ALERTING_THRESHOLD_MS);
  }

  private ConfigEntry mockConfigEntry(String name, String value, ConfigEntry.ConfigSource configSource) {
    ConfigEntry configEntry = EasyMock.mock(ConfigEntry.class);
    EasyMock.expect(configEntry.name()).andReturn(name).anyTimes();
    EasyMock.expect(configEntry.value()).andReturn(value).anyTimes();
    EasyMock.expect(configEntry.source()).andReturn(configSource).anyTimes();
    EasyMock.replay(configEntry);
    return configEntry;
  }

  @SuppressWarnings("unchecked")
  private void expectDescribeBrokerConfigs(AdminClient adminClient, int brokerId, Config brokerConfig) {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    Map<ConfigResource, Config> configs = Collections.singletonMap(cf, brokerConfig);
    DescribeConfigsResult mockResult = EasyMock.mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> mockFuture = EasyMock.mock(KafkaFuture.class);
    try {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(configs);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    EasyMock.expect(mockResult.all()).andReturn(mockFuture);
    EasyMock.expect(adminClient.describeConfigs(Collections.singletonList(cf))).andReturn(mockResult);
    EasyMock.replay(mockResult, mockFuture);
  }

  @SuppressWarnings("unchecked")
  private void expectIncrementalAlterBrokerConfigs(AdminClient adminClient, int brokerId) {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    AlterConfigsResult mockResult = EasyMock.mock(AlterConfigsResult.class);
    KafkaFuture<Void> mockFuture = EasyMock.mock(KafkaFuture.class);
    try {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    EasyMock.expect(mockResult.all()).andReturn(mockFuture);
    EasyMock.expect(adminClient.incrementalAlterConfigs(
        Collections.singletonMap(cf, EasyMock.anyObject()))).andReturn(mockResult);
    EasyMock.replay(mockResult, mockFuture);
  }
}
