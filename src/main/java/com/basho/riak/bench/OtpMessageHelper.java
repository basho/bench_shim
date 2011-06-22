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

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * @author russell
 * 
 */
public class OtpMessageHelper {
    /**
     * Create a reply, either an atom or a tuple, depending on length of
     * <code>values</code>
     * 
     * @param string2
     * @return an {@link OtpErlangAtom} or an {@link OtpErlangTuple} or
     *         {@link OtpErlangAtom}s
     */
    public static OtpErlangObject reply(String... values) {
        if (values.length == 1) {
            return new OtpErlangAtom(values[0]);
        } else {
            OtpErlangObject[] contents = new OtpErlangObject[values.length];

            for (int i = 0; i < values.length; i++) {
                contents[i] = new OtpErlangAtom(values[i]);
            }

            return new OtpErlangTuple(contents);
        }
    }
}
