package edu.itu.csc550.homework;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ValidatorMapper extends Mapper<Object, Text, Text, Text> {

    private LongWritable previousKey;
    private String filename;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //Part file is starting
    	String[] line = value.toString().split("\\s");
    	LongWritable linekey = new LongWritable(Long.parseLong(line[0]));
    	
        if(previousKey == null){
            //Initialize variables
            filename = ((FileSplit)context.getInputSplit()).getPath().getName();
            context.write(new Text(filename + ":begin"), new Text(line[0]));
            previousKey = new LongWritable();
        }else{
            if(linekey.compareTo(previousKey) < 0){
                context.write(new Text("error"), new Text("filename: " + filename + " previousKey: "+ previousKey + " lineKey: "+ linekey.toString()));
            }
        }

        previousKey.set(linekey.get());
    }

    @Override
    public void cleanup(Context context)
            throws IOException, InterruptedException  {
        if (previousKey != null) {
            context.write(new Text(filename + ":end"), new Text(previousKey.toString()));
        }
    }

}
