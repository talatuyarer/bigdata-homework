package edu.itu.csc550.homework;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link DummyInputFormat} will create dummy files as many as number of maps
 */
public class DummyInputFormat extends InputFormat<Text,Text> {
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) {
        List<InputSplit> result = new ArrayList<>();
        Path outDir = FileOutputFormat.getOutputPath(jobContext);
        int numSplits = jobContext.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
        for(int i=0; i < numSplits; ++i) {
            result.add(new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1, null));
        }
        return result;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        return new DummyRecordReader(((FileSplit) inputSplit).getPath());
    }
}
