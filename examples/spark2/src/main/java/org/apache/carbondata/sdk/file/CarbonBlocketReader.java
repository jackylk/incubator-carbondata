package org.apache.carbondata.sdk.file;

import org.apache.hadoop.mapreduce.RecordReader;

import java.util.List;

public class CarbonBlocketReader<T> extends CarbonReader<T> {
    private String blocketId;

    public CarbonBlocketReader(List<RecordReader<Void, T>> readers) {
        super(readers);
    }

    public CarbonBlocketReader(List<RecordReader<Void, T>> readers, String blocketId) {
        super(readers);
        this.blocketId = blocketId;
    }

    public String getBlocketId() {
        return blocketId;
    }
}
