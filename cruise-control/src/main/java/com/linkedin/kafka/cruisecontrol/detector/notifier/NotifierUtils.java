/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import java.io.IOException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotifierUtils {

    private static final Logger LOG = LoggerFactory.getLogger(NotifierUtils.class);

    private NotifierUtils() { }

    /**
     * Method used to send POST message to a specific URL (application/json)
     */
    public static void sendMessage(String message, String postUrl, String authorisationKey) throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(postUrl);
        StringEntity entity = new StringEntity(message);
        httpPost.setEntity(entity);
        if (authorisationKey != null) {
            httpPost.setHeader("Authorization", "Key " + authorisationKey);
        }
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        try {
            LOG.debug("Sending alert to: {}\nBody:\n{}", httpPost, message);
            CloseableHttpResponse httpResponse = client.execute(httpPost);
            LOG.debug("Response status: {}", httpResponse.getStatusLine().getStatusCode());
        } finally {
            client.close();
        }
    }

}
