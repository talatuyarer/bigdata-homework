package edu.itu.csc550.homework;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * {@link DummyRecordReader} will create empty key value pairs like as (dummyfilename,"")
 */
public class DummyRecordReader extends RecordReader<Text, Text> {
    private Path name;
    private Text key;

    public DummyRecordReader(Path path){
        this.name = path;
    }
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {

    }

    @Override
    public boolean nextKeyValue() {
        if (name != null) {
            key = new Text();
            key.set(name.getName());
            name = null;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return new Text();
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void close() {

    }
}
