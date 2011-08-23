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

import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;

import com.basho.riak.client.util.ClientUtils;
import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangRangeException;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * @author russell
 * 
 */
public class PutArgs {

    private final byte[] bucket;
    private final byte[] key;
    private final byte[] value;
    private final int w;
    private final int dw;

    /**
     * @param bucket
     * @param key
     * @param value
     * @param w
     * @param dw
     */
    public PutArgs(byte[] bucket, byte[] key, byte[] value, int w, int dw) {
        this.bucket = bucket;
        this.key = key;
        this.value = value;
        this.w = w;
        this.dw = dw;
    }

    /**
     * @return the bucket
     */
    public String getBucket() {
        return new String(Base64.encodeBase64Chunked(bucket), ClientUtils.ISO_8859_1);
    }

    /**
     * @return the key
     */
    public String getKey() {
        return new String(Base64.encodeBase64Chunked(key), ClientUtils.ISO_8859_1);
    }

    /**
     * @return the value
     */
    public byte[] getValue() {
        return value;
    }

    /**
     * @return the w
     */
    public int getW() {
        return w;
    }

    /**
     * @return the dw
     */
    public int getDw() {
        return dw;
    }

    /**
     * Avert your eyes.
     * 
     * @param args
     * @return
     */
    public static PutArgs from(final OtpErlangList args) {
        byte[] bucket = new byte[0];
        byte[] key = new byte[0];
        byte[] value = new byte[0];
        int w = 0;
        int dw = 0;

        for (OtpErlangObject arg : args) {
            if (!(arg instanceof OtpErlangTuple)) {
                throw new IllegalArgumentException(arg.toString() + " is not a Tuple");
            }

            OtpErlangTuple argTuple = (OtpErlangTuple) arg;

            if (argTuple.arity() != 2) {
                throw new IllegalArgumentException(argTuple.toString() + " is not a 2 tuple");
            }

            if (((OtpErlangAtom) argTuple.elementAt(0)).atomValue().equals("bucket")) {
                bucket = ((OtpErlangBinary) argTuple.elementAt(1)).binaryValue();
            }

            if (((OtpErlangAtom) argTuple.elementAt(0)).atomValue().equals("key")) {
                key = ((OtpErlangBinary) argTuple.elementAt(1)).binaryValue();
            }

            if (((OtpErlangAtom) argTuple.elementAt(0)).atomValue().equals("value")) {
                value = ((OtpErlangBinary) argTuple.elementAt(1)).binaryValue();
            }

            if (((OtpErlangAtom) argTuple.elementAt(0)).atomValue().equals("w")) {
                try {
                    w = ((OtpErlangLong) argTuple.elementAt(1)).intValue();
                } catch (OtpErlangRangeException e) {
                    throw new IllegalArgumentException(e);
                }
            }

            if (((OtpErlangAtom) argTuple.elementAt(0)).atomValue().equals("dw")) {
                try {
                    dw = ((OtpErlangLong) argTuple.elementAt(1)).intValue();
                } catch (OtpErlangRangeException e) {
                    throw new IllegalArgumentException(e);
                }
            }

        }

        return new PutArgs(bucket, key, value, w, dw);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override public String toString() {
        return String.format("PutArgs [bucket=%s, key=%s, value=%s, w=%s, dw=%s]", Arrays.toString(bucket),
                             Arrays.toString(key), Arrays.toString(value), w, dw);
    }

}
