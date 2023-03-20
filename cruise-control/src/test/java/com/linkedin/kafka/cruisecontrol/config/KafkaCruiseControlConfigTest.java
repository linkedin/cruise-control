/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.config;

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class KafkaCruiseControlConfigTest {

    private static final String KEY = "key";

    /**
     * getMap() should parse valid values.
     */
    @Test
    public void getMapParsesValidValues() {
        assertGetMapResultMatchesExpected("", Collections.emptyMap());
        assertGetMapResultMatchesExpected(" ", Collections.emptyMap());
        assertGetMapResultMatchesExpected(" ;", Collections.emptyMap());
        assertGetMapResultMatchesExpected("k=", Map.of("k", ""));
        assertGetMapResultMatchesExpected(" k = ", Map.of("k", ""));
        assertGetMapResultMatchesExpected("k=v", Map.of("k", "v"));
        assertGetMapResultMatchesExpected(" k = v ", Map.of("k", "v"));
        assertGetMapResultMatchesExpected("k1=v1;k2=v2a,v2b", Map.of("k1", "v1", "k2", "v2a,v2b"));
        assertGetMapResultMatchesExpected(" k1 = v1 ; k2 = v2 ", Map.of("k1", "v1", "k2", "v2"));
    }

    /**
     * getMap() should reject invalid values.
     */
    @Test
    public void getMapThrowsExceptionForInvalidValues() {
        assertGetMapThrowsConfigException("k");
        assertGetMapThrowsConfigException(" k ");
        assertGetMapThrowsConfigException("=");
        assertGetMapThrowsConfigException(" = ");
        assertGetMapThrowsConfigException("=v");
        assertGetMapThrowsConfigException(" = v ");
    }

    private void assertGetMapResultMatchesExpected(String rawValue,
            Map<String, String> expectedValue) {
        final KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(
                PermissiveConfigDef.of(KEY), Map.of(KEY, rawValue));
        assertThat(config.getMap(KEY), is(expectedValue));
    }

    private void assertGetMapThrowsConfigException(String rawValue) {
        final KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(
                PermissiveConfigDef.of(KEY), Map.of(KEY, rawValue));
        assertThrows(ConfigException.class, () -> config.getMap(KEY));
    }

    private static class PermissiveConfigDef extends ConfigDef {

        /**
         * Create an instance with a single key of type STRING.
         * @param key The config key name.
         * @return The new instance, ready for use with a KafkaCruiseControlConfig instance.
         */
        public static PermissiveConfigDef of(String key) {
            final PermissiveConfigDef result = new PermissiveConfigDef();
            result.define(key, Type.STRING, Importance.LOW, "");
            return result;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<String, Object> parse(Map<?, ?> props) {
            return (Map<String, Object>) props;
        }
    }
}
