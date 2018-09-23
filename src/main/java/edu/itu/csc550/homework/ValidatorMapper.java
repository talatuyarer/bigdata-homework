package edu.itu.csc550.homework;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ValidatorMapper extends Mapper<Object, Text, Text, Text> {

    private Text previousKey;
    private String filename;

    public void setup(Context context) {
    	System.out.println("Test");
    }
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //Part file is starting
    	String[] line = value.toString().split("\\s");
    	Text linekey = new Text(line[0]);
    	
        if(previousKey == null){
            //Initialize variables
            filename = ((FileSplit)context.getInputSplit()).getPath().getName();
            context.write(new Text(filename + ":start"), new Text(line[0]));
            previousKey = new Text();
        }else{
            if(linekey.compareTo(previousKey) < 0){
                context.write(new Text("error"), new Text("filename: " + filename + " previousKey: "+ previousKey + " lineKey: "+ linekey));
            }
        }

        previousKey.set(linekey);
    }

    @Override
    public void cleanup(Context context)
            throws IOException, InterruptedException  {
        if (previousKey != null) {
            context.write(new Text(filename + ":end"), previousKey);
        }
    }

}
