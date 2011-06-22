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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ericsson.otp.erlang.OtpMbox;
import com.ericsson.otp.erlang.OtpNode;

/**
 * Starts up the OtpNode and creates a {@link Factory} instance for creating
 * client {@link OtpMbox}en
 * 
 * @author russell
 * 
 */
public class BenchShim {

    private static BenchShim INSTANCE;

    private final OtpNode self;
    private final Factory factory;
    private final ExecutorService factoryExecutorService = Executors.newSingleThreadExecutor();

    private BenchShim(String name, String cookie) throws IOException {
        self = new OtpNode(name, cookie);
        factory = new Factory(self);
    }

    private BenchShim(String name) throws IOException {
        self = new OtpNode(name);
        factory = new Factory(self);
    }

    public static synchronized void run(String name, String cookie) throws IOException {
        if (INSTANCE == null) {
            INSTANCE = new BenchShim(name, cookie);
            INSTANCE.start();
        }
    }

    public static synchronized void run(String name) throws IOException {
        if (INSTANCE == null) {
            INSTANCE = new BenchShim(name);
            INSTANCE.start();
        }
    }

    private void start() {
        factoryExecutorService.execute(factory);
    }

}
