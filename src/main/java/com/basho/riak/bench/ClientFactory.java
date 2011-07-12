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

import com.basho.riak.client.http.RiakConfig;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.http.HTTPClientAdapter;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.pbc.RiakClient;

/**
 * @author russell
 * 
 */
public class ClientFactory {

    /**
     * @param config
     * @return
     * @throws IOException
     */
    public static RawClient newClient(ClientConfig config) throws IOException {
        RawClient client = null;
        Transport transport = config.getTransport();

        switch (transport) {
        case PB:
            client = new PBClientAdapter(new RiakClient(config.getHost(), config.getPort(), config.getBufferSizeKb()));
            break;
        case HTTP:
            RiakConfig conf = new RiakConfig(makeUrl(config.getHost(), config.getPort()));
            com.basho.riak.client.http.RiakClient del = new com.basho.riak.client.http.RiakClient(conf);
            client = new HTTPClientAdapter(del);
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
