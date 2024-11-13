/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata.kafka;

public class KafkaPersistedMapException extends RuntimeException {

    public KafkaPersistedMapException(String message, Exception cause) {
        super(message, cause);
    }
}
