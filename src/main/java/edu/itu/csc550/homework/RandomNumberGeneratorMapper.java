package edu.itu.csc550.homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

/**
 * {@link RandomNumberGeneratorMapper} creates random key and value records between range.
 */
public class RandomNumberGeneratorMapper extends Mapper<WritableComparable, Writable, IntWritable, IntWritable> {
    //Constants
    private static final String MB_PER_MAP = "homework.megabytes-per-map";
    private static final String MIN_KEY = "homework.minimum-key-number";
    private static final String MAX_KEY = "homework.maximum-key-number";
    private static final String MIN_VALUE = "homework.minimum-value-number";
    private static final String MAX_VALUE = "homework.maximum-value-number";

    private long numBytesToWrite;
    private int minKeyLength;
    private int keySizeRange;
    private int minValueSize;
    private int valueSizeRange;
    private Random random = new Random();
    private IntWritable randomKey = new IntWritable();
    private IntWritable randomValue = new IntWritable();

    enum Counters { RECORDS_WRITTEN, BYTES_WRITTEN }

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        numBytesToWrite = 1024 * 1024 * conf.getLong(MB_PER_MAP,2 * conf.getInt(MRJobConfig.IO_SORT_MB, 512));
        minKeyLength = conf.getInt(MIN_KEY, 10);
        keySizeRange = conf.getInt(MAX_KEY, 1000) - minKeyLength;
        minValueSize = conf.getInt(MIN_VALUE, 0);
        valueSizeRange = conf.getInt(MAX_VALUE, 20000) - minValueSize;
    }

    public void map(WritableComparable key, Writable value, Context context) throws IOException,InterruptedException {
        //Holding total record count
        int recordCount = 0;
        while (numBytesToWrite > 0) {
            //Generate Random values for key and value
            randomKey.set(minKeyLength + (keySizeRange != 0 ? random.nextInt(keySizeRange) : 0));
            randomValue.set(minValueSize + (valueSizeRange != 0 ? random.nextInt(valueSizeRange) : 0));
            context.write(randomKey, randomValue);
            //Reduce from total bytes
            numBytesToWrite -= 4;

            //Update counters
            context.getCounter(Counters.BYTES_WRITTEN).increment(4);
            context.getCounter(Counters.RECORDS_WRITTEN).increment(1);
            if (++recordCount % 1000 == 0) {
                context.setStatus(recordCount + " are written. " + numBytesToWrite + " bytes left.");
            }
        }
        //We generated enough records for this map
        context.setStatus("Totally " + recordCount + " records are written.");
    }
}
