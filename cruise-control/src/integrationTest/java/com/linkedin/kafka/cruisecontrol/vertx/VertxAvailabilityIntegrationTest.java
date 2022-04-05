/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.vertx;
import com.linkedin.kafka.cruisecontrol.CruiseControlVertxIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class VertxAvailabilityIntegrationTest extends CruiseControlVertxIntegrationTestHarness {
    @Before
    public void start() throws Exception {
        super.start();
    }

    @After
    public void teardown() {
        super.stop();
    }

    @Test
    public void testVertxApiGetEndpointsAvailability() throws Exception {
        for (CruiseControlEndPoint endpoint : CruiseControlEndPoint.getEndpoints()) {
            Integer responseCode = getResponseCode(endpoint.toString().toLowerCase());
            assertTrue(399 > responseCode || responseCode >= 500);
        }
    }

    @Test
    public void testVertxApiPostEndpointsAvailability() throws Exception {
        for (CruiseControlEndPoint endpoint : CruiseControlEndPoint.postEndpoints()) {
            Integer responseCode = getResponseCode(endpoint.toString().toLowerCase());
            assertTrue(399 > responseCode || responseCode >= 500 || responseCode == 405);
        }
    }

    @Test
    public void testSwaggerUiAvailability() throws Exception {
        Integer responseCode = getResponseCode("");
        assertTrue(399 >= responseCode || responseCode >= 500);
    }
}
