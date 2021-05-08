/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

/**
 * @see <a href="https://docs.alerta.io/en/latest/conventions.html#event-groups">the Alerta event groups conventions</a>
 */
public enum AlertaAlertGroup {
    PERFORMANCE("Performance"), STORAGE("Storage");

    private final String _value;

    AlertaAlertGroup(String value) {
        this._value = value;
    }

    @Override
    public String toString() {
        return _value;
    }
}
