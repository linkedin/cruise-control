/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.exception;

/**
 * If an error occurs while retrieving the topic description,
 * or if the topic name retrieval method cannot be found or invoked properly. This includes
 * exceptions related to reflection (e.g., {@link NoSuchMethodException}), invocation issues,
 * execution exceptions, timeouts, and interruptions.
 */
public class KafkaTopicDescriptionException extends Exception {

    public KafkaTopicDescriptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
