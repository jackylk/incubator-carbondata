package org.apache.carbondata.sdk.file;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope;

public class SimpleFilesReadCommittedScope extends LatestFilesReadCommittedScope {
    private String indexFile;

    public SimpleFilesReadCommittedScope(String path, String segmentId, String indexFile) {
        super(path, segmentId);
        this.indexFile = indexFile;
    }

    public SimpleFilesReadCommittedScope(String path, String indexFile) {
        super(path);
        this.indexFile = indexFile;
    }

    @Override
    protected CarbonFile[] filterCarbonFiles(CarbonFile[] allFiles) {
        if (indexFile == null) {
            return super.filterCarbonFiles(allFiles);
        }
        CarbonFile[] carbonFiles = new CarbonFile[1];
        for (CarbonFile carbonFile: allFiles) {
            if (carbonFile.getAbsolutePath().endsWith(indexFile)) {
                carbonFiles[0] = carbonFile;
            }
        }
        return carbonFiles;
    }
}
