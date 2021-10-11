/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.BrokerFailureDetector.BROKER_FAILURES_FIXABLE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.BrokerFailureDetector.FAILED_BROKERS_OBJECT_CONFIG;

public class AlertaSelfHealingNotifierTest {
    private static BrokerFailures failures;
    private static Time mockTime;
    private MockAlertaSelfHealingNotifier _notifier;

    public static final String DUMMY_ALERTA_API_URL = "http://dummy.alerta.api";
    public static final String DUMMY_ALERTA_API_KEY = "dummy-alerta-api-key";
    public static final String DUMMY_ALERTA_ENV = "MyEnv";

    public static final String TEST_DATETIME_1 = "2021-03-24 10:01:01Z";
    public static final String TEST_DATETIME_ALARM_1 = "2021-03-24T10:01:01.000Z";
    public static final String TEST_DATETIME_2 = "2021-03-24 09:05:03Z";
    public static final String TEST_DATETIME_ALARM_2 = "2021-03-24T09:05:03.000Z";

    /**
     * Setup the test.
     * @throws ParseException
     */
    @BeforeClass
    public static void setup() throws ParseException {
        final long startTime = 500L;
        mockTime = new MockTime(0, startTime, TimeUnit.NANOSECONDS.convert(startTime, TimeUnit.MILLISECONDS));
        KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
        Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
        KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
        EasyMock.expect(mockKafkaCruiseControl.config()).andReturn(kafkaCruiseControlConfig).anyTimes();
        EasyMock.replay(mockKafkaCruiseControl);
        Map<Integer, Long> failedBrokers = Map.of(1, new SimpleDateFormat("yyyy-M-dd hh:mm:ssX").parse(TEST_DATETIME_1).getTime(),
                                                  2, new SimpleDateFormat("yyyy-M-dd hh:mm:ssX").parse(TEST_DATETIME_2).getTime());
        Map<String, Object> parameterConfigOverrides = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, mockKafkaCruiseControl,
                                                              ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, 200L,
                                                              FAILED_BROKERS_OBJECT_CONFIG, failedBrokers,
                                                              BROKER_FAILURES_FIXABLE_CONFIG, true);
        failures = kafkaCruiseControlConfig.getConfiguredInstance(AnomalyDetectorConfig.BROKER_FAILURES_CLASS_CONFIG,
                                                                  BrokerFailures.class,
                                                                  parameterConfigOverrides);
    }

    @Test
    public void testAlertaAlertWithNoWebhook() {
        _notifier = new MockAlertaSelfHealingNotifier(mockTime);
        _notifier.alert(failures, false, 1L, KafkaAnomalyType.BROKER_FAILURE);
        assertEquals(0, _notifier.getAlertaMessageList().size());
    }

    @Test
    public void testAlertaAlertWithNoApiKey() {
        _notifier = new MockAlertaSelfHealingNotifier(mockTime);
        _notifier._alertaApiUrl = DUMMY_ALERTA_API_URL;
        _notifier.alert(failures, false, 1L, KafkaAnomalyType.BROKER_FAILURE);
        assertEquals(0, _notifier.getAlertaMessageList().size());
    }

    @Test
    public void testAlertaAlertWithDefaultOptions() {
        _notifier = new MockAlertaSelfHealingNotifier(mockTime);
        _notifier._alertaApiUrl = DUMMY_ALERTA_API_URL;
        _notifier._alertaApiKey = DUMMY_ALERTA_API_KEY;
        _notifier.alert(failures, false, 1L, KafkaAnomalyType.BROKER_FAILURE);
        assertEquals(2, _notifier.getAlertaMessageList().size());
    }

    @Test
    public void testAlertaAlertWithEnvironment() throws ParseException {
        _notifier = new MockAlertaSelfHealingNotifier(mockTime);
        _notifier._alertaApiUrl = DUMMY_ALERTA_API_URL;
        _notifier._alertaApiKey = DUMMY_ALERTA_API_KEY;
        _notifier._alertaEnvironment = DUMMY_ALERTA_ENV;
        long time = new SimpleDateFormat("yyyy-M-dd hh:mm:ssX").parse("2021-03-24 10:02:01Z").getTime();
        _notifier.alert(failures, false, time, KafkaAnomalyType.BROKER_FAILURE);
        assertEquals(2, _notifier.getAlertaMessageList().size());
        assertEquals(KafkaAnomalyType.BROKER_FAILURE.toString(), _notifier.getAlertaMessageList().get(0).getEvent());
        assertEquals(KafkaAnomalyType.BROKER_FAILURE.toString(), _notifier.getAlertaMessageList().get(1).getEvent());
        assertEquals(_notifier._alertaEnvironment, _notifier.getAlertaMessageList().get(0).getEnvironment());
        // Depends on the underneath Java implementation, TEST_DATETIME_ALARM_1 can either be in the front of the list,
        if (TEST_DATETIME_ALARM_1.equals(_notifier.getAlertaMessageList().get(0).getCreateTime())) {
          assertEquals(TEST_DATETIME_ALARM_2, _notifier.getAlertaMessageList().get(1).getCreateTime());
        } else {
          // Or, in the back of the list. We flip the get order in this case.
          assertEquals(TEST_DATETIME_ALARM_1, _notifier.getAlertaMessageList().get(1).getCreateTime());
          assertEquals(TEST_DATETIME_ALARM_2, _notifier.getAlertaMessageList().get(0).getCreateTime());
        }
    }

    private static class MockAlertaSelfHealingNotifier extends AlertaSelfHealingNotifier {
        private final List<AlertaMessage> _alertaMessageList;

        MockAlertaSelfHealingNotifier(Time time) {
            super(time);
            _selfHealingEnabled.put(KafkaAnomalyType.BROKER_FAILURE, true);
            _alertaMessageList = new ArrayList<>();
        }

        @Override
        protected void sendAlertaMessage(AlertaMessage alertaMessage) {
          _alertaMessageList.add(alertaMessage);
        }

        List<AlertaMessage> getAlertaMessageList() {
            return _alertaMessageList;
        }
    }

}
