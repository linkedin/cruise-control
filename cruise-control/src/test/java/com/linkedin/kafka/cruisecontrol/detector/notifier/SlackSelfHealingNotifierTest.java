/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import java.util.Properties;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.BrokerFailureDetector.BROKER_FAILURES_FIXABLE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.BrokerFailureDetector.FAILED_BROKERS_OBJECT_CONFIG;
import static org.junit.Assert.assertEquals;

public class SlackSelfHealingNotifierTest {
    private static BrokerFailures failures;
    private static Time mockTime;
    private MockSlackSelfHealingNotifier _notifier;

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
        Map<Integer, Long> failedBrokers = Map.of(1, 200L, 2, 400L);
        Map<String, Object> parameterConfigOverrides = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, mockKafkaCruiseControl,
                                                              ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, 200L,
                                                              FAILED_BROKERS_OBJECT_CONFIG, failedBrokers,
                                                              BROKER_FAILURES_FIXABLE_CONFIG, true);
        failures = kafkaCruiseControlConfig.getConfiguredInstance(AnomalyDetectorConfig.BROKER_FAILURES_CLASS_CONFIG,
                                                                  BrokerFailures.class,
                                                                  parameterConfigOverrides);
    }

    @Test
    public void testSlackAlertWithNoWebhook() {
        _notifier = new MockSlackSelfHealingNotifier(mockTime);
        _notifier.alert(failures, false, 1L, KafkaAnomalyType.BROKER_FAILURE);
        assertEquals(0, _notifier.getSlackMessageList().size());
    }

    @Test
    public void testSlackAlertWithNoChannel() {
        _notifier = new MockSlackSelfHealingNotifier(mockTime);
        _notifier._slackWebhook = "http://dummy.slack.webhook";
        _notifier.alert(failures, false, 1L, KafkaAnomalyType.BROKER_FAILURE);
        assertEquals(0, _notifier.getSlackMessageList().size());
    }

    @Test
    public void testSlackAlertWithDefaultOptions() {
        _notifier = new MockSlackSelfHealingNotifier(mockTime);
        _notifier._slackWebhook = "http://dummy.slack.webhook";
        _notifier._slackChannel = "#dummy-channel";
        _notifier.alert(failures, false, 1L, KafkaAnomalyType.BROKER_FAILURE);
        assertEquals(1, _notifier.getSlackMessageList().size());
        SlackMessage message = _notifier.getSlackMessageList().get(0);
        assertEquals("#dummy-channel", message.getChannel());
    }

    private static class MockSlackSelfHealingNotifier extends SlackSelfHealingNotifier {
        private final List<SlackMessage> _slackMessageList;

        MockSlackSelfHealingNotifier(Time time) {
            super(time);
            _selfHealingEnabled.put(KafkaAnomalyType.BROKER_FAILURE, true);
            _slackMessageList = new ArrayList<>();
        }

        @Override
        protected void sendSlackMessage(SlackMessage slackMessage, String slackWebhookUrl) {
            _slackMessageList.add(slackMessage);
        }

        List<SlackMessage> getSlackMessageList() {
            return _slackMessageList;
        }
    }

}
