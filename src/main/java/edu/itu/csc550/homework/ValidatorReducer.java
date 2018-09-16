package edu.itu.csc550.homework;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class ValidatorReducer extends Reducer<Text,Text,Text,Text> {
    private boolean firstKey = true;
    private Text lastKey = new Text();
    private Text lastValue = new Text();
    private Text ERROR = new Text("error");

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
      if (ERROR.equals(key)) {
        for (Text val : values) {
          context.write(key, val);
        }
      } else {
        Text value = values.iterator().next();
        if (firstKey) {
          firstKey = false;
        } else {
          if (value.compareTo(lastValue) < 0) {
            context.write(ERROR, new Text("file " + lastKey + " key " + lastValue + "\n  file " + key + " key " + value));
          }
        }
        lastKey.set(key);
        lastValue.set(value);
      }
    }
  }