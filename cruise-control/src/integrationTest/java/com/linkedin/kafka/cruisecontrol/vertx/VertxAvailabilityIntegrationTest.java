/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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
            assertTrue(399 > getResponseCode(endpoint.toString().toLowerCase()) || getResponseCode(endpoint.toString().toLowerCase()) >= 500);
        }
    }

    @Test
    public void testVertxApiPostEndpointsAvailability() throws Exception {
        for (CruiseControlEndPoint endpoint : CruiseControlEndPoint.postEndpoints()) {
            assertTrue(399 > getResponseCode(endpoint.toString().toLowerCase())
                    || getResponseCode(endpoint.toString().toLowerCase()) >= 500 || getResponseCode(endpoint.toString().toLowerCase()) == 405);
        }
    }

    @Test
    public void testSwaggerUiAvailability() throws Exception {
        assertTrue(399 >= getResponseCode("") || getResponseCode("") >= 500);
    }
}
