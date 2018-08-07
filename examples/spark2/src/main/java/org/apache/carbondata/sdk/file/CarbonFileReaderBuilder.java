package org.apache.carbondata.sdk.file;

import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class CarbonFileReaderBuilder<T> {

    public static interface FileFilter {
        boolean filter(CarbonInputSplit inputSplit);
    }
    private String tablePath;
    private String indexFile;
//    private String tableName;
//    private String dbName;
    private String[] projectionColumns;
    private Expression filterExpression;
    private FileFilter fileFilter;

    public CarbonFileReaderBuilder(String tablePath) {
        this.tablePath = tablePath;
//        this.tableName = tableName;
//        this.dbName = dbName;
    }

    public CarbonFileReaderBuilder projection(String[] projectionColumnNames) {
        this.projectionColumns = projectionColumnNames;
        return this;
    }

    public CarbonFileReaderBuilder filter(Expression filterExpression) {
        this.filterExpression = filterExpression;
        return this;
    }

    public CarbonFileReaderBuilder filter(FileFilter fileFilter) {
        this.fileFilter = fileFilter;
        return this;
    }

    public CarbonFileReaderBuilder indexFile(String indexFile) {
        this.indexFile = indexFile;
        return this;
    }

    public <T> List<CarbonBlocketReader<T>> build() throws IOException, InterruptedException {
        CarbonTable table;
        table = CarbonTable.buildDummyTable(tablePath);
        final CarbonFileInputFormat format = new SimpleCarbonFileInputFormat(indexFile);

        final Job job = new Job(new Configuration());
        format.setTableInfo(job.getConfiguration(), table.getTableInfo());
        format.setTablePath(job.getConfiguration(), table.getTablePath());
        format.setTableName(job.getConfiguration(), table.getTableName());
        format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());
        if (filterExpression != null) {
            format.setFilterPredicates(job.getConfiguration(), filterExpression);
        }

        if (projectionColumns != null) {
            // set the user projection
            format.setColumnProjection(job.getConfiguration(), projectionColumns);
        }

        try {
            final List<InputSplit> splits =
                    format.getSplits(new JobContextImpl(job.getConfiguration(), new JobID()));

            List<InputSplit> resultSplit;
            if (fileFilter != null) {
                resultSplit = new ArrayList<>();
                for (InputSplit split: splits) {
                    if (split instanceof CarbonInputSplit && fileFilter.filter((CarbonInputSplit)split)) {
                        resultSplit.add(split);
                    }
                }
            } else {
                resultSplit = splits;
            }

            List<CarbonBlocketReader<T>> carbonReaders = new ArrayList<>();
            for (InputSplit split: resultSplit) {
                TaskAttemptContextImpl attempt =
                        new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
                RecordReader reader = format.createRecordReader(split, attempt);
                List<RecordReader<Void, T>> readers = new ArrayList<>();
                try {
                    reader.initialize(split, attempt);
                    readers.add(reader);
                    carbonReaders.add(new CarbonBlocketReader<>(readers, ((CarbonInputSplit)split).getBlockletId()));
                } catch (Exception e) {
                    reader.close();
                    throw e;
                }
            }
            return carbonReaders;
        } catch (Exception ex) {
            // Clear the datamap cache as it can get added in getSplits() method
            DataMapStoreManager.getInstance()
                    .clearDataMaps(table.getAbsoluteTableIdentifier());
            throw ex;
        }
    }
}
