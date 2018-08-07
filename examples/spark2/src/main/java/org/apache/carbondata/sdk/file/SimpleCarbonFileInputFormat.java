package org.apache.carbondata.sdk.file;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope;
import org.apache.carbondata.hadoop.api.CarbonFileInputFormat;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class SimpleCarbonFileInputFormat<T> extends CarbonFileInputFormat<T> {
    private String indexFile;

    public SimpleCarbonFileInputFormat(String indexFile) {
        this.indexFile = indexFile;
    }

    @Override
    public CarbonTable getOrCreateCarbonTable(Configuration configuration) throws IOException {
        CarbonTable carbonTable = super.getOrCreateCarbonTable(configuration);
        return carbonTable;
    }

    @Override
    protected LatestFilesReadCommittedScope getLatestFilesReadCommittedScope(String tablePath) {
        return new SimpleFilesReadCommittedScope(tablePath, indexFile);
    }
}
