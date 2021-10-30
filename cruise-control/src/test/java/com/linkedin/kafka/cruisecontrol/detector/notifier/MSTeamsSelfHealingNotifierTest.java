/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.BrokerFailureDetector.BROKER_FAILURES_FIXABLE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.BrokerFailureDetector.FAILED_BROKERS_OBJECT_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MSTeamsSelfHealingNotifierTest {
    private static BrokerFailures failures;
    private static Time mockTime;
    private MockMSTeamsSelfHealingNotifier _notifier;

    /**
     * Setup the test.
     */
    @BeforeClass
    public static void setup() {
        final long startTime = 500L;
        mockTime = new MockTime(0, startTime, TimeUnit.NANOSECONDS.convert(startTime, TimeUnit.MILLISECONDS));
        KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
        Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
        KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
        EasyMock.expect(mockKafkaCruiseControl.config()).andReturn(kafkaCruiseControlConfig).anyTimes();
        EasyMock.replay(mockKafkaCruiseControl);
        Map<Integer, Long> failedBrokers = new HashMap<>();
        failedBrokers.put(1, 200L);
        failedBrokers.put(2, 400L);
        Map<String, Object> parameterConfigOverrides = new HashMap<>(4);
        parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, mockKafkaCruiseControl);
        parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, 200L);
        parameterConfigOverrides.put(FAILED_BROKERS_OBJECT_CONFIG, failedBrokers);
        parameterConfigOverrides.put(BROKER_FAILURES_FIXABLE_CONFIG, true);
        failures = kafkaCruiseControlConfig.getConfiguredInstance(AnomalyDetectorConfig.BROKER_FAILURES_CLASS_CONFIG,
                                                                  BrokerFailures.class,
                                                                  parameterConfigOverrides);
    }

    @Test
    public void testMSTeamsAlertWithNoWebhook() {
        _notifier = new MockMSTeamsSelfHealingNotifier(mockTime);
        _notifier.alert(failures, false, 1L, KafkaAnomalyType.BROKER_FAILURE);
        assertEquals(0, _notifier.getMSTeamsMessageList().size());
    }

    @Test
    public void testMSTeamsAlertWithDefaultOptions() {
        _notifier = new MockMSTeamsSelfHealingNotifier(mockTime);
        _notifier._msTeamsWebhook = "https://dummy.webhook.office.com/webhookb2";
        _notifier.alert(failures, false, 1L, KafkaAnomalyType.BROKER_FAILURE);
        assertEquals(1, _notifier.getMSTeamsMessageList().size());
        MSTeamsMessage message = _notifier.getMSTeamsMessageList().get(0);
        assertNotNull(message.getFacts().get("Anomaly"));
        assertEquals("true", message.getFacts().get("Self Healing enabled"));
        assertEquals("false", message.getFacts().get("Auto fix triggered"));

    }

    private static class MockMSTeamsSelfHealingNotifier extends MSTeamsSelfHealingNotifier {
        private final List<MSTeamsMessage> _msTeamsMessageList;

        MockMSTeamsSelfHealingNotifier(Time time) {
            super(time);
            _selfHealingEnabled.put(KafkaAnomalyType.BROKER_FAILURE, true);
            _msTeamsMessageList = new ArrayList<>();
        }

        @Override
        protected void sendMSTeamsMessage(MSTeamsMessage msTeamsMessage, String msTeamsWebhookUrl) {
            _msTeamsMessageList.add(msTeamsMessage);
        }

        List<MSTeamsMessage> getMSTeamsMessageList() {
            return _msTeamsMessageList;
        }
    }

}
