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
import java.util.ArrayList;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpErlangExit;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangPid;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;
import com.ericsson.otp.erlang.OtpMbox;

/**
 * Wraps an {@link OtpMbox} and a {@link RawClient}, handles comms between the
 * two
 *
 * @author russell
 *
 */
public class ClientShim implements Runnable {

    private final OtpMbox mbox;
    private final IRiakClient riakClient;
    private final String host;

    /**
     * @param mbox
     *            the {@link OtpMbox} that will receive messages from
     *            basho_bench for this client
     * @param host
     *            the host to connect to
     * @param port
     *            the port to connect to
     * @param bufferSizeKb
     *            the pb buffer size
     * @param transport
     *            the {@link Transport} to create (http/pb/httpcluster/pbcluster)
     * @throws IOException
     * @throws RiakException
     */
    public ClientShim(final OtpMbox mbox, String host, int port, int bufferSizeKb, Transport transport)
            throws IOException, RiakException {
        this.mbox = mbox;
        switch (transport) {
        case PB:
            final PBClientConfig pbClientConf = new PBClientConfig.Builder().withHost(host).withPort(port).withSocketBufferSizeKb(bufferSizeKb).build();
            this.riakClient = RiakFactory.newClient(pbClientConf);
            break;
        case HTTP:
            final HTTPClientConfig httpClientConf = new HTTPClientConfig.Builder().withHost(host).withPort(port).build();
            this.riakClient = RiakFactory.newClient(httpClientConf);
            break;
        default:
            throw new RuntimeException("unknown transport " + transport);
        }

        this.riakClient.generateAndSetClientId();
        this.host = host;
    }

    /**
     * @param mbox
     *            the {@link OtpMbox} that will receive messages from
     *            basho_bench for this client
     * @param nodes
     *            the set of nodes to use for a cluster
     * @param bufferSizeKb
     *            the pb buffer size
     * @param transport
     *            the {@link Transport} to create (httpcluster/pbcluster)
     * @throws IOException
     * @throws RiakException
     */
    public ClientShim(final OtpMbox mbox, ArrayList<Node> nodes, int bufferSizeKb, Transport transport)
            throws IOException, RiakException {
        this.mbox = mbox;
        this.host = null;
        switch (transport) {
        case PB:
        case PBCLUSTER:
            final PBClusterConfig pbClusterConf = new PBClusterConfig(nodes.size());
            for (Node node : nodes) {
                PBClientConfig nodeConf = new PBClientConfig.Builder().withHost(node.getHost()).withPort(node.getPort()).withSocketBufferSizeKb(bufferSizeKb).build();
                pbClusterConf.addClient(nodeConf);
            }
            this.riakClient = RiakFactory.newClient(pbClusterConf);
            break;
        case HTTP:
        case HTTPCLUSTER:
            final HTTPClusterConfig httpClusterConf = new HTTPClusterConfig(nodes.size());
            for (Node node : nodes) {
                HTTPClientConfig nodeConf = new HTTPClientConfig.Builder().withHost(node.getHost()).withPort(node.getPort()).build();
                httpClusterConf.addClient(nodeConf);
            }
            this.riakClient = RiakFactory.newClient(httpClusterConf);
            break;
        default:
            throw new RuntimeException("unknown transport " + transport);
        }

        this.riakClient.generateAndSetClientId();
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
     */
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                OtpErlangTuple msg = (OtpErlangTuple) mbox.receive();

                OtpErlangPid from = (OtpErlangPid) msg.elementAt(0);
                OtpErlangTuple payload = (OtpErlangTuple) msg.elementAt(1);
                OtpErlangAtom operation = (OtpErlangAtom) payload.elementAt(0);
                OtpErlangList args = (OtpErlangList) payload.elementAt(1);

                final Op op = Op.fromString(operation.atomValue());

                OtpErlangObject reply = null;

                // TODO really could be just Args, amirite?
                final PutArgs putArgs = PutArgs.from(args);
                final GetArgs getArgs = GetArgs.from(args);

                switch (op) {
                case GET:
                    try {
                        Bucket bucket = riakClient.createBucket(getArgs.getBucket()).execute();
                        IRiakObject object = bucket.fetch(getArgs.getKey()).r(getArgs.getR()).execute();

                        if (object == null || ((object.getValue() == null) && (object.getVClock() == null))) {
                            // send not found
                            reply = reply("ok", "notfound");
                        } else {
                            // send found message back
                            reply = reply("ok", "found");
                        }
                    } catch (Exception e) {
                        // send error message
                        reply = errorReply(e, putArgs, getArgs);
                    }
                    break;
                case PUT:
                    try {
                        Bucket bucket = riakClient.createBucket(putArgs.getBucket()).execute();
                        bucket.store(putArgs.getKey(), putArgs.getValue())
                        .w(putArgs.getW())
                        .dw(putArgs.getDw())
                        .returnBody(false);
                        reply = reply("ok");
                    } catch (Exception e) {
                        reply = errorReply(e, putArgs, getArgs);
                    }
                    break;
                case DELETE:
                    try {
                        Bucket bucket = riakClient.createBucket(getArgs.getBucket()).execute();
                        bucket.delete(getArgs.getKey()).rw(getArgs.getR()).execute();
                        reply = reply("ok");
                    } catch (RiakRetryFailedException e) {
                        reply = errorReply(e, putArgs, getArgs);
                    }
                    break;
                case CREATE_UPDATE:
                    try {
                        Bucket bucket = riakClient.createBucket(getArgs.getBucket()).execute();
                        IRiakObject object = bucket.fetch(getArgs.getKey()).r(getArgs.getR()).execute();

                        if (object == null || ((object.getValue() == null) && (object.getVClock() == null))) {
                            // send not found
                            reply = reply("ok", "notfound");
                        } else {
                            // send found message back
                            reply = reply("ok", "found");
                        }

                        if (object != null && ((object.getValue() != null) && (object.getVClock() != null))) {
                            bucket.store(getArgs.getKey(), putArgs.getValue())
                            .w(putArgs.getW())
                            .dw(putArgs.getDw())
                            .returnBody(false)
                            .execute();
                        }

                        reply = reply("ok");
                    } catch (Exception e) {
                        reply = errorReply(e, putArgs, getArgs);
                    }

                    break;
                case UPDATE:
                    try {
                        Bucket bucket = riakClient.createBucket(getArgs.getBucket()).execute();
                        IRiakObject object = bucket.fetch(getArgs.getKey()).r(getArgs.getR()).execute();

                        if (object == null || ((object.getValue() == null) && (object.getVClock() == null))) {
                            // send not found
                            reply = reply("error", "notfound");
                        } else {
                            bucket.store(getArgs.getKey(), putArgs.getValue())
                            .w(putArgs.getW())
                            .dw(putArgs.getDw())
                            .returnBody(false)
                            .execute();
                            reply = reply("ok");
                        }
                    } catch (Exception e) {
                        reply = errorReply(e, putArgs, getArgs);
                    }

                    break;
                default:
                    throw new UnsupportedOperationException(op.name());
                }
                mbox.send(from, new OtpErlangTuple(new OtpErlangObject[] { mbox.self(), reply }));
            } catch (OtpErlangExit e) {
                System.out.println(e.pid().toString() + " has exited with reason: " + e.reason().toString());
            } catch (OtpErlangDecodeException e) {
                throw new RuntimeException(e);
            }
        }
        mbox.exit("interupted");
    }

    /**
     * @param e
     * @param putArgs
     * @param getArgs
     * @return
     */
    private OtpErlangObject errorReply(Exception e, PutArgs putArgs, GetArgs getArgs) {
        OtpErlangAtom error = new OtpErlangAtom("error");
        String eString = e.toString() + " b : " + putArgs.getBucket() + " k : " + putArgs.getKey();
        OtpErlangString reason = new OtpErlangString(eString);

        System.out.println("sending error message for " + host + " :: " + eString);
        return new OtpErlangTuple(new OtpErlangObject[] { error, reason });
    }

}
