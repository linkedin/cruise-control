/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata.namespace;

import com.linkedin.kafka.cruisecontrol.persisteddata.namespace.ExecutorPersistedData.BrokerMapKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

public class ExecutorPersistedDataTest {

    private static final int SINGLE_KEY = 1;
    private static final long SINGLE_VALUE = 1L;
    private static final Map<Integer, Long> SINGLE_ENTRY_MAP = Map.of(SINGLE_KEY, SINGLE_VALUE);
    private ExecutorPersistedData _executorData;

    /**
     * Set up the object under test, when the object is needed.
     */
    public void setUp() {
        _executorData = new ExecutorPersistedData(new HashMap<>());
    }

    /**
     * Helper for ensuring serialized data is deserialized back to be the same as the original
     * data.
     *
     * @param data data to serialize then compare against the deserialized version.
     */
    private static void assertThatSerializedDataCanBeDeserialized(Map<Integer, Long> data) {
        final String serialized = ExecutorPersistedData.serializeNumberMap(data);
        assertThat(ExecutorPersistedData.deserializeNumberMap(serialized), is(data));
    }

    /**
     * Ensure the executor's data is namespaced within the backing map.
     */
    @Test
    public void executorDataIsCorrectlyNamespacedWithinTheBackingStore() {
        final HashMap<String, String> backingMap = new HashMap<>();
        final ExecutorPersistedData executorData = new ExecutorPersistedData(backingMap);
        assertThat(backingMap.size(), is(0));
        final Map<Integer, Long> demotedBrokers = executorData.getDemotedOrRemovedBrokers(
                BrokerMapKey.DEMOTE);
        demotedBrokers.put(1, 2L);
        executorData.setDemotedOrRemovedBrokers(BrokerMapKey.DEMOTE, demotedBrokers);
        assertThat(backingMap.size(), is(1));
        final String key = backingMap.keySet().stream().findFirst().orElseThrow();
        assertThat(key, containsString(Namespace.EXECUTOR.toString().toLowerCase()));
    }

    /**
     * A null should come back as a null.
     */
    @Test
    public void serializeAndDeserializeNumberMapWorksForNull() {
        assertThatSerializedDataCanBeDeserialized(null);
    }

    /**
     * An empty map should come back as an empty one.
     */
    @Test
    public void serializeAndDeserializeNumberMapWorksForEmptyMap() {
        assertThatSerializedDataCanBeDeserialized(Collections.emptyMap());
    }

    /**
     * A map with a single entry should work.
     */
    @Test
    public void serializeAndDeserializeNumberMapWorksForSingleValueMap() {
        assertThatSerializedDataCanBeDeserialized(SINGLE_ENTRY_MAP);
    }

    /**
     * Multiple entries and values along the entire number range should work.
     */
    @Test
    public void serializeAndDeserializeNumberMapWorksForMultiValueMap() {
        assertThatSerializedDataCanBeDeserialized(Map.of(
                Integer.MIN_VALUE, Long.MIN_VALUE,
                0, 0L,
                Integer.MAX_VALUE, Long.MAX_VALUE));
    }

    /**
     * Invalid serialized number map data should be ignored.
     */
    @Test
    public void deserializeNumberMapReturnsNullForInvalidSerializedData() {
        assertThat(ExecutorPersistedData.deserializeNumberMap("1"), is(Collections.emptyMap()));
        assertThat(ExecutorPersistedData.deserializeNumberMap("1:a"), is(Collections.emptyMap()));
        assertThat(ExecutorPersistedData.deserializeNumberMap("a:1"), is(Collections.emptyMap()));
    }

    /**
     * The first request for a key should return an empty map rather than null.
     */
    @Test
    public void getDemotedOrRemovedBrokersReturnsNonNullMap() {
        setUp();
        assertThat(_executorData.getDemotedOrRemovedBrokers(BrokerMapKey.DEMOTE), notNullValue());
    }

    /**
     * Data written using setDemotedOrRemovedBrokers() should readable using
     * getDemotedOrRemovedBrokers() for the same key without mixing up the keys.
     */
    @Test
    public void setDemotedOrRemovedBrokersDataCanBeGottenBack() {
        setUp();
        _executorData.setDemotedOrRemovedBrokers(BrokerMapKey.DEMOTE, SINGLE_ENTRY_MAP);
        assertThat(_executorData.getDemotedOrRemovedBrokers(BrokerMapKey.DEMOTE),
                is(SINGLE_ENTRY_MAP));
        assertThat(_executorData.getDemotedOrRemovedBrokers(BrokerMapKey.REMOVE).isEmpty(),
                is(true));
    }

    /**
     * The lock objects need to be consistent for locking to work correctly.
     */
    @Test
    public void getDemotedOrRemovedBrokersLockReturnsSameObjectInstanceEachTime() {
        setUp();
        assertThat(_executorData.getDemotedOrRemovedBrokersLock(BrokerMapKey.DEMOTE),
                sameInstance(_executorData.getDemotedOrRemovedBrokersLock(BrokerMapKey.DEMOTE)));
    }

    /**
     * Each key needs its own lock object for locking to work correctly.
     */
    @Test
    public void getDemotedOrRemovedBrokersLockReturnsUniqueInstanceForEachKey() {
        setUp();
        assertThat(_executorData.getDemotedOrRemovedBrokersLock(BrokerMapKey.DEMOTE),
                not(sameInstance(
                        _executorData.getDemotedOrRemovedBrokersLock(BrokerMapKey.REMOVE))));
    }

    /**
     * modifyDemotedOrRemovedBrokers() must apply the modify function to the key, and only the
     * key's, map.
     */
    @Test
    public void modifyDemotedOrRemovedBrokersSetsTheModifiedValueForAKey() {
        setUp();
        _executorData.modifyDemotedOrRemovedBrokers(BrokerMapKey.DEMOTE,
                map -> map.put(SINGLE_KEY, SINGLE_VALUE));
        assertThat(_executorData.getDemotedOrRemovedBrokers(BrokerMapKey.DEMOTE),
                is(SINGLE_ENTRY_MAP));
        assertThat(_executorData.getDemotedOrRemovedBrokers(BrokerMapKey.REMOVE).isEmpty(),
                is(true));
    }

    /**
     * Multiple threads reading/writing the same data should do so atomically.
     */
    @Test
    public void modifyDemotedOrRemovedBrokersSynchronizesModifications()
            throws InterruptedException {
        setUp();
        _executorData.setDemotedOrRemovedBrokers(BrokerMapKey.DEMOTE, SINGLE_ENTRY_MAP);

        // Have 2 workers continually increment the same key/value. The result should be the same
        // as if they ran in series.
        final int numWorkers = 2;
        final int numIterations = 10;
        Thread[] workers = new Thread[numWorkers];
        final ThreadFactory threadFactory = Executors.defaultThreadFactory();
        for (int i = 0; i < workers.length; i++) {
            workers[i] = threadFactory.newThread(() -> {
                for (int j = 0; j < numIterations; j++) {
                    _executorData.modifyDemotedOrRemovedBrokers(BrokerMapKey.DEMOTE,
                            map -> map.compute(SINGLE_KEY, (k, v) -> v == null ? 0 : v + 1));
                }
            });
            workers[i].start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertThat(_executorData.getDemotedOrRemovedBrokers(BrokerMapKey.DEMOTE).get(SINGLE_KEY),
                is(SINGLE_VALUE + numWorkers * numIterations));
    }
}
