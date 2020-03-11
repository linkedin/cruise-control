/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toDateString;

public class SlackSelfHealingNotifier extends SelfHealingNotifier {

    private static final Logger LOG = LoggerFactory.getLogger(SlackSelfHealingNotifier.class);
    public static final String SLACK_SELF_HEALING_NOTIFIER_WEBHOOK = "slack.self.healing.notifier.webhook";
    public static final String SLACK_SELF_HEALING_NOTIFIER_ICON = "slack.self.healing.notifier.icon";
    public static final String SLACK_SELF_HEALING_NOTIFIER_USER = "slack.self.healing.notifier.user";
    public static final String SLACK_SELF_HEALING_NOTIFIER_CHANNEL = "slack.self.healing.notifier.channel";

    public static final String DEFAULT_SLACK_SELF_HEALING_NOTIFIER_ICON = ":information_source:";
    public static final String DEFAULT_SLACK_SELF_HEALING_NOTIFIER_USER = "Cruise Control";

    protected String _slackWebhook;
    protected String _slackIcon;
    protected String _slackChannel;
    protected String _slackUser;

    public SlackSelfHealingNotifier() {
    }

    public SlackSelfHealingNotifier(Time time) {
        super(time);
    }

    @Override
    public void configure(Map<String, ?> config) {
        super.configure(config);
        _slackWebhook = (String) config.get(SLACK_SELF_HEALING_NOTIFIER_WEBHOOK);
        _slackIcon = (String) config.get(SLACK_SELF_HEALING_NOTIFIER_ICON);
        _slackChannel = (String) config.get(SLACK_SELF_HEALING_NOTIFIER_CHANNEL);
        _slackUser = (String) config.get(SLACK_SELF_HEALING_NOTIFIER_USER);
        _slackIcon = _slackIcon == null ? DEFAULT_SLACK_SELF_HEALING_NOTIFIER_ICON : _slackIcon;
        _slackUser = _slackUser == null ? DEFAULT_SLACK_SELF_HEALING_NOTIFIER_USER : _slackUser;
    }

    @Override
    public void alert(Object anomaly, boolean autoFixTriggered, long selfHealingStartTime, AnomalyType anomalyType) {
        super.alert(anomaly, autoFixTriggered, selfHealingStartTime, anomalyType);

        if (_slackWebhook == null) {
            LOG.warn("Slack webhook is null, can't send Slack self healing notification");
            return;
        }

        if (_slackChannel == null) {
            LOG.warn("Slack channel name is null, can't send Slack self healing notification");
            return;
        }

        String text = String.format("%s detected %s. Self healing %s.%s", anomalyType, anomaly,
                _selfHealingEnabled.get(anomalyType) ? String.format("start time %s", toDateString(selfHealingStartTime))
                        : "is disabled",
                autoFixTriggered ? "%nSelf-healing has been triggered." : "");

        try {
            sendSlackMessage(new SlackMessage(_slackUser, text, _slackIcon, _slackChannel), _slackWebhook);
        } catch (IOException e) {
            LOG.warn("ERROR sending alert to Slack", e);
        }
    }

    protected void sendSlackMessage(SlackMessage slackMessage, String slackWebhookUrl) throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(slackWebhookUrl);
        StringEntity entity = new StringEntity(slackMessage.toString());
        httpPost.setEntity(entity);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        try {
            client.execute(httpPost);
        } finally {
            client.close();
        }
    }
}
