package com.linkedin.kafka.cruisecontrol.detector.notifier;

import java.io.Serializable;

public final class SlackMessage implements Serializable {

    private String username;
    private String text;
    private String icon_emoji;
    private String channel;

    public SlackMessage(String username, String text, String icon_emoji, String channel) {
        this.username = username;
        this.text = text;
        this.icon_emoji = icon_emoji;
        this.channel = channel;
    }

    public String getUsername() {
        return username;
    }

    public String getText() {
        return text;
    }

    public String getIcon_emoji() {
        return icon_emoji;
    }

    public String getChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return "{\"username\" : " + (username == null ? null : "\"" + username + "\"")
                + ",\"text\" : " + (text == null ? null : "\"" + text + "\"")
                + ",\"icon_emoji\" : " + (icon_emoji == null ? null : "\"" + icon_emoji + "\"")
                + ",\"channel\" : " + (channel == null ? null : "\"" + channel + "\"") + "}";
    }
}
