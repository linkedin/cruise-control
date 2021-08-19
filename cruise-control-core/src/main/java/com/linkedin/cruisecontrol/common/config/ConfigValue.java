/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.cruisecontrol.common.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


public class ConfigValue {

    private final String _name;
    private Object _value;
    private List<Object> _recommendedValues;
    private final List<String> _errorMessages;
    private boolean _visible;

    public ConfigValue(String name) {
        this(name, null, new ArrayList<>(), new ArrayList<>());
    }

    public ConfigValue(String name, Object value, List<Object> recommendedValues, List<String> errorMessages) {
        _name = name;
        _value = value;
        _recommendedValues = recommendedValues;
        _errorMessages = errorMessages;
        _visible = true;
    }

    public String name() {
        return _name;
    }

    public Object value() {
        return _value;
    }

    public void value(Object value) {
        this._value = value;
    }

    public List<Object> recommendedValues() {
        return Collections.unmodifiableList(_recommendedValues);
    }

    public void recommendedValues(List<Object> recommendedValues) {
        this._recommendedValues = recommendedValues;
    }

    public List<String> errorMessages() {
        return Collections.unmodifiableList(_errorMessages);
    }

    public boolean visible() {
        return _visible;
    }

    public void visible(boolean visible) {
        this._visible = visible;
    }

    public void addErrorMessage(String errorMessage) {
        this._errorMessages.add(errorMessage);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConfigValue that = (ConfigValue) o;
        return Objects.equals(_name, that._name) && Objects.equals(_value, that._value)
               && Objects.equals(_recommendedValues, that._recommendedValues) && Objects.equals(_errorMessages, that._errorMessages)
               && Objects.equals(_visible, that._visible);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_name, _value, _recommendedValues, _errorMessages, _visible);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[")
            .append(_name)
            .append(",")
            .append(_value)
            .append(",")
            .append(_recommendedValues)
            .append(",")
            .append(_errorMessages)
            .append(",")
            .append(_visible)
            .append("]");
        return sb.toString();
    }
}
