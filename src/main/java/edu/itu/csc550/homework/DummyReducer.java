package edu.itu.csc550.homework;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class DummyReducer extends Reducer<IntWritable, IntWritable, WritableComparable, Writable> {
    
	@Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Do nothing
    	for(IntWritable value: values) {
    		context.write(key, value);
    	}
    }

}
