/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.rocksdb;

import java.nio.charset.Charset;

/** Merge operator that concatenates two strings with a delimiter of variable length, string or byte array. */
public class StringAppendOperatorWithVariableDelimitor extends MergeOperator {
    public StringAppendOperatorWithVariableDelimitor() {
        this(',');
    }

    public StringAppendOperatorWithVariableDelimitor(char delim) {
        this(Character.toString(delim));
    }

    public StringAppendOperatorWithVariableDelimitor(byte[] delim) {
        super(newSharedStringAppendTESTOperator(delim));
    }

    public StringAppendOperatorWithVariableDelimitor(String delim) {
        this(delim.getBytes());
    }

    public StringAppendOperatorWithVariableDelimitor(String delim, Charset charset) {
        this(delim.getBytes(charset));
    }

    private native static long newSharedStringAppendTESTOperator(final byte[] delim);
    @Override protected final native void disposeInternal(final long handle);
}