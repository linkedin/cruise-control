/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import java.io.Serializable;

public final class SlackMessage implements Serializable {

    private String username;
    private String text;
    private String iconEmoji;
    private String channel;

    public SlackMessage(String username, String text, String iconEmoji, String channel) {
        this.username = username;
        this.text = text;
        this.iconEmoji = iconEmoji;
        this.channel = channel;
    }

    public String getUsername() {
        return username;
    }

    public String getText() {
        return text;
    }

    public String getIconEmoji() {
        return iconEmoji;
    }

    public String getChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return "{\"username\" : " + (username == null ? null : "\"" + username + "\"")
                + ",\"text\" : " + (text == null ? null : "\"" + text + "\"")
                + ",\"icon_emoji\" : " + (iconEmoji == null ? null : "\"" + iconEmoji + "\"")
                + ",\"channel\" : " + (channel == null ? null : "\"" + channel + "\"") + "}";
    }
}
