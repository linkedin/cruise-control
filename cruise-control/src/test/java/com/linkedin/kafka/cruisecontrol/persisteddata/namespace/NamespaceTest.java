/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata.namespace;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class NamespaceTest {

    /**
     * All, and only, keys mapped by the mapper should be retrievable by the back-mapper.
     */
    @Test
    public void namespaceKeyMapperFunctionIsOneToOneReversible() {
        final String key = "key";
        for (Namespace namespace : Namespace.values()) {
            String mappedKey = namespace.keyToNamespaceMapper(key);

            // The mapped key should map back to the key.
            assertThat(namespace.namespaceToKeyMapper(mappedKey), is(key));

            // An unmapped key should map to null to indicate key failed the back-mapper's
            // validation.
            assertThat(namespace.namespaceToKeyMapper(key), is(nullValue()));
        }
    }

    /**
     * Make sure embed() returns a wrapped view of the provided map that keeps keys and values in
     * their own key namespace.
     */
    @Test
    public void embedReturnsNamespacedViewOfMap() {
        final String key = "key";
        final String value = "value";
        final String embeddedValue = "embeddedValue";
        final Namespace namespace = Namespace.EXECUTOR;
        Map<String, String> map = new HashMap<>();
        Map<String, String> embeddedMap = namespace.embed(map);
        map.put(key, value);
        embeddedMap.put(key, embeddedValue);
        assertThat(map.get(key), is(value));
        assertThat(embeddedMap.get(key), is(embeddedValue));
    }
}
