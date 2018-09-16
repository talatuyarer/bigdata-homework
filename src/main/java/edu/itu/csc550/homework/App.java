package edu.itu.csc550.homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;

/**
 * Hello world!
 *
 */
public class App extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception {
        int result = ToolRunner.run(new Configuration(), new App(), args);
        System.exit(result);
    }

    /**
     * Execute the command with the given arguments.
     *
     * @param args command specific arguments.
     * @return exit code.
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Path outDir = new Path(App.class.getName() + System.currentTimeMillis());

        Configuration conf = getConf();

        int ioSortMb = conf.getInt(MRJobConfig.IO_SORT_MB, 512);
        int mapMb = Math.max(2 * ioSortMb, conf.getInt(MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB));
        conf.setInt(MRJobConfig.NUM_MAPS, 10 * 1024 / mapMb);
        conf.setInt(MRJobConfig.MAP_MEMORY_MB, mapMb);
        conf.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx" + (mapMb - 200) + "m");

        Job job = Job.getInstance(conf);
        job.setJarByClass(App.class);
        job.setJobName("sort-homework");
        FileOutputFormat.setOutputPath(job, outDir);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(DummyInputFormat.class);
        job.setMapperClass(RandomNumberGeneratorMapper.class);
        job.setReducerClass(DummyReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(1);

        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        int ret = job.waitForCompletion(true) ? 0 : 1;
        Date endTime = new Date();
        System.out.println("Job ended: " + endTime);
        System.out.println("The job's latency is " + (endTime.getTime() - startTime.getTime()) /1000 + " seconds.");
        return ret;
    }
}
