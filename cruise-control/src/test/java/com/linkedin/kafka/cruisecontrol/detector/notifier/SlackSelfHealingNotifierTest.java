package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SlackSelfHealingNotifierTest {

    private static BrokerFailures failures;
    private static KafkaCruiseControl mockKafkaCruiseControl;
    private static MockSlackSelfHealingNotifier notifier;

    @BeforeClass
    public static void setup() {
        final long startTime = 500L;
        Time mockTime = new MockTime(0, startTime, TimeUnit.NANOSECONDS.convert(startTime, TimeUnit.MILLISECONDS));
        notifier = new MockSlackSelfHealingNotifier(mockTime);
        mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
        Map<Integer, Long> failedBrokers = new HashMap<>();
        failedBrokers.put(1, 200L);
        failedBrokers.put(2, 400L);
        failures = new BrokerFailures(mockKafkaCruiseControl, failedBrokers, true, true, true);
    }

    @Test
    public void testSlackAlertWithNoWebhook() {
        notifier.alert(failures, false, 1L, AnomalyType.BROKER_FAILURE);
        assertEquals(0, notifier.getSlackMessageList().size());
    }

    @Test
    public void testSlackAlertWithNoChannel() {
        notifier._slackWebhook = "http://dummy.slack.webhook";
        notifier.alert(failures, false, 1L, AnomalyType.BROKER_FAILURE);
        assertEquals(0, notifier.getSlackMessageList().size());
    }

    @Test
    public void testSlackAlertWithDefaultOptions() {
        notifier._slackWebhook = "http://dummy.slack.webhook";
        notifier._slackChannel = "#dummy-channel";
        notifier.alert(failures, false, 1L, AnomalyType.BROKER_FAILURE);
        assertEquals(1, notifier.getSlackMessageList().size());
        SlackMessage message = notifier.getSlackMessageList().get(0);
        assertEquals("#dummy-channel", message.getChannel());
    }

    private static class MockSlackSelfHealingNotifier extends SlackSelfHealingNotifier {
        private static List<SlackMessage> slackMessageList = new ArrayList<>();

        final Map<AnomalyType, Boolean> _alertCalled;
        final Map<AnomalyType, Boolean> _autoFixTriggered;

        MockSlackSelfHealingNotifier(Time time) {
            super(time);
            _alertCalled = new HashMap<>(AnomalyType.cachedValues().size());
            _autoFixTriggered = new HashMap<>(AnomalyType.cachedValues().size());
            for (AnomalyType alertType : AnomalyType.cachedValues()) {
                _alertCalled.put(alertType, false);
                _autoFixTriggered.put(alertType, false);
            }
            _selfHealingEnabled.put(AnomalyType.BROKER_FAILURE, true);
        }


        @Override
        protected void sendSlackMessage(SlackMessage slackMessage, String slackWebhookUrl) throws IOException {
            slackMessageList.add(slackMessage);
        }

        List<SlackMessage> getSlackMessageList() {
            return slackMessageList;
        }
    }

}
