/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class InFlightRequestsTest {

    private InFlightRequests inFlightRequests;
    private int correlationId;
    private Node destination = new Node(0, "host", 0);

    @Before
    public void setup() {
        inFlightRequests = new InFlightRequests(12);
        correlationId = 0;
    }

    @Test
    public void testCompleteLastSent() {
        int correlationId1 = addRequest(destination);
        int correlationId2 = addRequest(destination);
        assertEquals(2, inFlightRequests.count());

        assertEquals(correlationId2, inFlightRequests.completeLastSent(destination.idString()).header.correlationId());
        assertEquals(1, inFlightRequests.count());

        assertEquals(correlationId1, inFlightRequests.completeLastSent(destination.idString()).header.correlationId());
        assertEquals(0, inFlightRequests.count());
    }

    @Test
    public void testClearAll() {
        int correlationId1 = addRequest(destination);
        int correlationId2 = addRequest(destination);

        List<NetworkClient.InFlightRequest> clearedRequests = TestUtils.toList(this.inFlightRequests.clearAll(destination.idString()));
        assertEquals(0, inFlightRequests.count());
        assertEquals(2, clearedRequests.size());
        assertEquals(correlationId1, clearedRequests.get(0).header.correlationId());
        assertEquals(correlationId2, clearedRequests.get(1).header.correlationId());
    }

    @Test
    public void testCompleteNext() {
        int correlationId1 = addRequest(destination);
        int correlationId2 = addRequest(destination);
        assertEquals(2, inFlightRequests.count());

        assertEquals(correlationId1, inFlightRequests.completeNext(destination.idString()).header.correlationId());
        assertEquals(1, inFlightRequests.count());

        assertEquals(correlationId2, inFlightRequests.completeNext(destination.idString()).header.correlationId());
        assertEquals(0, inFlightRequests.count());
    }

    @Test(expected = IllegalStateException.class)
    public void testCompleteNextThrowsIfNoInflights() {
        inFlightRequests.completeNext(destination.idString());
    }

    @Test(expected = IllegalStateException.class)
    public void testCompleteLastSentThrowsIfNoInFlights() {
        inFlightRequests.completeLastSent(destination.idString());
    }

    private int addRequest(Node destination) {
        int correlationId = this.correlationId;
        this.correlationId += 1;

        RequestHeader requestHeader = new RequestHeader(ApiKeys.METADATA, (short) 0, "clientId", correlationId);
        NetworkClient.InFlightRequest ifr = new NetworkClient.InFlightRequest(requestHeader, 0,
                destination, null, false, false, null, null, 0);
        inFlightRequests.add(ifr);
        return correlationId;
    }

}
