package edu.itu.csc550.homework;

import com.sun.imageio.spi.FileImageInputStreamSpi;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.PureJavaCrc32;

import java.io.IOException;

public class ValidatorMapper extends Mapper<Text,Text, Text, Text> {

    private Text previousKey;
    private String filename;

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //Part file is starting
        if(previousKey == null){
            //Initialize variables
            filename = ((FileSplit)context.getInputSplit()).getPath().getName();
            context.write(new Text(filename + ":start"), key);
            previousKey = new Text();
        }else{
            if(key.compareTo(previousKey) < 0){
                context.write(new Text("error"), new Text("filename:" + filename));
            }
        }

        previousKey.set(key);
    }

    @Override
    public void cleanup(Context context)
            throws IOException, InterruptedException  {
        if (previousKey != null) {
            context.write(new Text(filename + ":end"), previousKey);
        }
    }

}
