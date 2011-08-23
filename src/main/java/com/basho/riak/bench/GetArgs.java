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
public class GetArgs {

    private final int r;
    private final byte[] bucket;
    private final byte[] key;

    /**
     * @param r
     * @param bucket
     * @param key
     */
    private GetArgs(int r, byte[] bucket, byte[] key) {
        this.r = r;
        this.bucket = bucket;
        this.key = key;
    }

    /**
     * @return the r
     */
    public int getR() {
        return r;
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
     * Avert your eyes.
     * 
     * @param args
     * @return
     */
    public static GetArgs from(final OtpErlangList args) {
        byte[] bucket = new byte[0];
        byte[] key = new byte[0];
        int r = 0;

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

            if (((OtpErlangAtom) argTuple.elementAt(0)).atomValue().equals("r")) {
                try {
                    r = ((OtpErlangLong) argTuple.elementAt(1)).intValue();
                } catch (OtpErlangRangeException e) {
                    throw new IllegalArgumentException(e);
                }
            }

        }

        return new GetArgs(r, bucket, key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override public String toString() {
        return String.format("GetArgs [r=%s, bucket=%s, key=%s]", r, Arrays.toString(bucket), Arrays.toString(key));
    }

}
