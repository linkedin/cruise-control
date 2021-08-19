/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import java.io.Serializable;

public final class SlackMessage implements Serializable {
    private final String _username;
    private final String _text;
    private final String _iconEmoji;
    private final String _channel;

    public SlackMessage(String username, String text, String iconEmoji, String channel) {
        _username = username;
        _text = text;
        _iconEmoji = iconEmoji;
        _channel = channel;
    }

    public String getUsername() {
        return _username;
    }

    public String getText() {
        return _text;
    }

    public String getIconEmoji() {
        return _iconEmoji;
    }

    public String getChannel() {
        return _channel;
    }

    @Override
    public String toString() {
        return "{\"username\" : " + (_username == null ? null : "\"" + _username + "\"")
                + ",\"text\" : " + (_text == null ? null : "\"" + _text + "\"")
                + ",\"icon_emoji\" : " + (_iconEmoji == null ? null : "\"" + _iconEmoji + "\"")
                + ",\"channel\" : " + (_channel == null ? null : "\"" + _channel + "\"") + "}";
    }
}
