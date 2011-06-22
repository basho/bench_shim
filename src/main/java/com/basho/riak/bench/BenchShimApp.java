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

/**
 * Starts up a node the will start mboxen for local Riak Java client instances
 * <p>
 * Usage -- <code>
 * <pre>
 * BenchDriverApp [nodeName cookie]
 * </pre>
 * </code>
 * </p>
 * <p>
 * I run it with mvn like this <code>
 * <pre>
 *  mvn exec:java -Dexec.mainClass="com.basho.riak.bench.BenchDriverApp" -Dexec.classpathScope=runtime -Dexec.args="java@myhost.com mySecretCookie"
 * </pre>
 * </code>
 * </p>
 * 
 * @author russell
 * 
 */
public class BenchShimApp {

    /**
     */
    public static void main(String[] args) {
        String nodeName = "java_client";
        String cookie = null;

        if (args.length > 0) {
            nodeName = args[0];
        }

        if (args.length > 1) {
            cookie = args[1];
        }

        try {
            if (cookie == null) {
                BenchShim.run(nodeName);
            } else {
                BenchShim.run(nodeName, cookie);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
