/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbon.flink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public final class ProxyRecoverableSerializer implements SimpleVersionedSerializer<ProxyRecoverable> {

    static final ProxyRecoverableSerializer INSTANCE = new ProxyRecoverableSerializer();

    private static final int VERSION = 1;

    private ProxyRecoverableSerializer() {
        // to do nothing.
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(final ProxyRecoverable proxyRecoverable) {
        return new byte[0];
    }

    @Override
    public ProxyRecoverable deserialize(final int i, final byte[] bytes) {
        return new ProxyRecoverable();
    }

}
