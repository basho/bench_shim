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

import static com.basho.riak.bench.OtpMessageHelper.reply;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpErlangExit;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangPid;
import com.ericsson.otp.erlang.OtpErlangRangeException;
import com.ericsson.otp.erlang.OtpErlangTuple;
import com.ericsson.otp.erlang.OtpMbox;
import com.ericsson.otp.erlang.OtpNode;

/**
 * Wraps the {@link OtpMbox} for receiving "create" messages and an executor for
 * running client threads
 * 
 * @author russell
 * 
 */
public class Factory implements Runnable {

    private final OtpMbox mbox;
    private final OtpNode node;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    /**
     * @param mbox
     */
    public Factory(final OtpNode node) {
        this.mbox = node.createMbox("factory");
        this.node = node;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final OtpErlangTuple msg = (OtpErlangTuple) mbox.receive();
                final OtpErlangPid from = (OtpErlangPid) msg.elementAt(0);
                final OtpErlangTuple payload = (OtpErlangTuple) msg.elementAt(1);
                final String host = getHost((OtpErlangTuple) payload.elementAt(0));
                OtpErlangObject reply;
                int port;
                int bufferSizeKb;

                try {
                    port = ((OtpErlangLong) payload.elementAt(1)).intValue();
                    bufferSizeKb =  ((OtpErlangLong) payload.elementAt(2)).intValue();

                    try {
                        // create a new mbox and client, get a Pid to send back
                        reply = newClientShim(host, port, bufferSizeKb);
                    } catch (IOException e) {
                        // we couldn't create a client, tell the sender
                        reply = reply("error", e.toString());
                    }
                } catch (OtpErlangRangeException e) {
                    // Port causes a range exception, tell the sender
                    reply = reply("error", e.getMessage());
                }

                mbox.send(from, reply);
            } catch (OtpErlangExit e) {
                shutdown();
                throw new RuntimeException(e);
            } catch (OtpErlangDecodeException e) {
                // no-one to reply too, no need to die either...log it?
                e.printStackTrace();
            }
        }
        shutdown();
    }

    /**
     * clean up
     */
    private void shutdown() {
        executorService.shutdown();
        mbox.exit("interrupted");
    }

    /**
     * Get a string representation of a host from an erlang ip tuple Erlang EG
     * if tuple is {127,0,0,1} then return is "127.0.0.1"
     * 
     * @param hostTuple
     * @return string representation of an erlang ip tuple
     */
    private String getHost(OtpErlangTuple hostTuple) {
        final StringBuilder host = new StringBuilder();

        boolean first = true;

        for (OtpErlangObject o : hostTuple.elements()) {
            if (!first) {
                host.append(".");
            }
            host.append(o.toString());
            first = false;
        }
        return host.toString();
    }

    /**
     * Create a {@link ClientShim} runnable and execute it with the
     * {@link ExecutorService}
     * 
     * @param host
     *            that the client should connect to
     * @param port
     *            that the client should connect to
     * @return the {@link OtpErlangPid} of a new {@link OtpMbox} created to
     *         handle messages for the new client
     * @throws IOException
     */
    private OtpErlangPid newClientShim(final String host, final int port, final int bufferSizeKb) throws IOException {
        System.out.println("Spawning new mbox for " + host + ":" + port + " with buffer " + bufferSizeKb);
        OtpMbox mbox = node.createMbox();
        OtpErlangPid pid = mbox.self();
        executorService.execute(new ClientShim(mbox, host, port, bufferSizeKb));
        return pid;
    }
}
