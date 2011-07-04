/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.bench;

/**
 * @author russell
 * 
 */
public class ClientConfig {

    private final String host;
    private final int port;
    private final Transport transport;
    private final int bufferSizeKb;

    /**
     * @param host
     * @param port
     * @param transport
     * @param bufferSizeKb
     */
    public ClientConfig(String host, int port, Transport transport, int bufferSizeKb) {
        this.host = host;
        this.port = port;
        this.transport = transport;
        this.bufferSizeKb = bufferSizeKb;
    }

    /**
     * @return the host
     */
    public synchronized String getHost() {
        return host;
    }

    /**
     * @return the port
     */
    public synchronized int getPort() {
        return port;
    }

    /**
     * @return the transport
     */
    public synchronized Transport getTransport() {
        return transport;
    }

    /**
     * @return the bufferSizeKb
     */
    public synchronized int getBufferSizeKb() {
        return bufferSizeKb;
    }
}
