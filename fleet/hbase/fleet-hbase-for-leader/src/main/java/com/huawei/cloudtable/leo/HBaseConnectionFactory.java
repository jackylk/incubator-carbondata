
package com.huawei.cloudtable.leo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public abstract class HBaseConnectionFactory {

    public static final HBaseConnectionFactory BUILD_IN = new HBaseConnectionFactory() {

        @Override
        public Connection newConnection(final Configuration configuration) throws IOException {
            return ConnectionFactory.createConnection(configuration);
        }

    };

    public abstract Connection newConnection(Configuration configuration) throws IOException;

}
