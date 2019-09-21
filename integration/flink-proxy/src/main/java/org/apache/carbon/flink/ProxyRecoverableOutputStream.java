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

import java.io.IOException;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;

public final class ProxyRecoverableOutputStream extends RecoverableFsDataOutputStream {

    ProxyRecoverableOutputStream(final String path) {
        this.path = path;
    }

    private final String path;

    private BulkWriter<?> writer;

    public String getPath() {
        return this.path;
    }

    @Override
    public long getPos() {
        throw new UnsupportedOperationException();
    }

    public void setWriter(final BulkWriter<?> writer) {
        this.writer = writer;
    }

    @Override
    public RecoverableWriter.ResumeRecoverable persist() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Committer closeForCommit() {
        return new ProxyRecoverableOutputStream.Committer(new ProxyRecoverable(), this.writer);
    }

    @Override
    public void write(int b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if (this.writer != null) {
            this.writer.finish();
        }
    }

    static final class Committer implements RecoverableFsDataOutputStream.Committer {

        Committer(final ProxyRecoverable recoverable, final BulkWriter<?> writer) {
            this.recoverable = recoverable;
            this.writer = writer;
        }

        private final ProxyRecoverable recoverable;

        private final BulkWriter<?> writer;

        @Override
        public void commit() throws IOException {
            if (this.writer != null) {
                this.writer.flush();
            }
        }

        @Override
        public void commitAfterRecovery() {
            // to do nothing.
        }

        @Override
        public RecoverableWriter.CommitRecoverable getRecoverable() {
            return this.recoverable;
        }

    }

}
