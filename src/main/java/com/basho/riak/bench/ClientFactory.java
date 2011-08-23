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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.basho.riak.client.RiakConfig;
import com.basho.riak.client.RiakClient;

/**
 * @author russell
 * 
 */
public class ClientFactory {

    private static final RiakClient httpClient;

    static {
        RiakConfig conf = new RiakConfig(makeUrl("den-test-01.den.basho", 80));
        httpClient = new RiakClient(conf);
    }

    /**
     * @param config
     * @return
     * @throws IOException
     */
    public static RiakClient newClient(ClientConfig config) throws IOException {
        RiakClient client = null;
        Transport transport = config.getTransport();

        switch (transport) {
        case PB:
            throw new UnsupportedOperationException("Only HTTP for 0.14.1 builds");
        case HTTP:
            client = httpClient;
            break;
        default:
            throw new RuntimeException("unknown transport " + transport);
        }

        return client;
    }

    /**
     * @param host
     * @param port
     * @return
     */
    private static String makeUrl(String host, int port) {
        final StringBuilder url = new StringBuilder("http://").append(host).append(":").append(port).append("/riak");
        return url.toString();
    }

}
