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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;

public class AuthenticationVerifier implements Runnable {

    private Thread callerThread;
    private Cluster cluster;
    private NetworkClient client;
    private long retryBackoffMs;

    public AuthenticationVerifier(Thread callerThread, Cluster cluster, NetworkClient client, long retryBackoffMs) {
        super();
        this.callerThread = callerThread;
        this.cluster = cluster;
        this.client = client;
        this.retryBackoffMs = retryBackoffMs;
    }

    @Override
    public void run() {
        try {
            while (true) {
                for (Node node: cluster.nodes()) {
                    if (client.authenticationFailed(node)) {
                        callerThread.interrupt();
                        Thread.currentThread().interrupt();
                    }
                }
                Thread.sleep(retryBackoffMs);
            }
        } catch (InterruptedException e) {
            // nothing to do, just shut down the thread
        }
    }
}
