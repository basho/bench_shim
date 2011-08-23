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

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.StoreResponse;
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
    private final RiakClient client;
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
     *            the {@link Transport} to create (http/pb)
     * @throws IOException
     */
    public ClientShim(final OtpMbox mbox, String host, int port, int bufferSizeKb, Transport transport)
            throws IOException {
        this.mbox = mbox;
        final ClientConfig clientConfig = new ClientConfig(host, port, transport, bufferSizeKb);
        this.client = ClientFactory.newClient(clientConfig);
        this.host = host;
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

                        FetchResponse response = client.fetch(getArgs.getBucket(), getArgs.getKey(),
                                                              RequestMeta.readParams(getArgs.getR()));

                        if (response.getStatusCode() == 404) {
                            // send not found
                            reply = reply("ok", "notfound");
                        } else if (response.isSuccess()) {
                            // send found message back
                            reply = reply("ok", "found");
                        } else {
                            reply = errorReply(new Exception(response.getBodyAsString()), putArgs, getArgs);
                        }
                    } catch (Exception e) {
                        // send error message
                        reply = errorReply(e, putArgs, getArgs);
                    }
                    break;
                case PUT:
                    try {
                        StoreResponse resp = client.store(new RiakObject(putArgs.getBucket(), putArgs.getKey(),
                                                                         putArgs.getValue()),
                                                          RequestMeta.writeParams(putArgs.getW(), putArgs.getDw()));

                        if (resp.isSuccess()) {
                            reply = reply("ok");
                        } else {
                            reply = errorReply(new Exception(resp.getBodyAsString()), putArgs, getArgs);
                        }
                    } catch (Exception e) {
                        reply = errorReply(e, putArgs, getArgs);
                    }
                    break;
                case DELETE:
                    try {
                        client.delete(getArgs.getBucket(), getArgs.getKey(), RequestMeta.readParams(getArgs.getR()));
                        reply = reply("ok");
                    } catch (Exception e) {
                        reply = errorReply(e, putArgs, getArgs);

                    }
                    break;
                case CREATE_UPDATE:
                    try {
                        RiakObject obj = null;
                        FetchResponse response = client.fetch(getArgs.getBucket(), getArgs.getKey(),
                                                              RequestMeta.readParams(getArgs.getR()));

                        if (response != null && response.hasObject()) {
                            obj = response.getObject();
                            obj.setValue(putArgs.getValue());
                        } else {
                            obj = new RiakObject(putArgs.getBucket(), putArgs.getKey(), putArgs.getValue());
                        }

                        StoreResponse resp = client.store(obj, RequestMeta.writeParams(putArgs.getW(), putArgs.getDw()));
                        
                        if (resp.isSuccess()) {
                            reply = reply("ok");
                        } else {
                            reply = errorReply(new Exception(resp.getBodyAsString()), putArgs, getArgs);
                        }
                    } catch (Exception e) {
                        reply = errorReply(e, putArgs, getArgs);
                    }

                    break;
                case UPDATE:
                    try {
                        FetchResponse response = client.fetch(getArgs.getBucket(), getArgs.getKey(),
                                                              RequestMeta.readParams(getArgs.getR()));
                        
                        if (!response.hasObject()) {
                            reply = reply("error", "notfound");
                        } else {
                            RiakObject obj = response.getObject();
                            obj.setValue(putArgs.getValue());
                            
                            StoreResponse resp = client.store(obj, RequestMeta.writeParams(putArgs.getW(), putArgs.getDw()));
                            
                            if (resp.isSuccess()) {
                                reply = reply("ok");
                            } else {
                                reply = errorReply(new Exception(resp.getBodyAsString()), putArgs, getArgs);
                            }
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
                throw new RuntimeException(e);
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
