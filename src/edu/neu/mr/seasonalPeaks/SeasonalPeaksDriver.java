package edu.neu.mr.seasonalPeaks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.neu.mr.utils.DateProductWritable;
import edu.neu.mr.utils.RatingCountWritable;


// map reduce driver function used to set the configuration
// of the job and read input parameters
// 5 input parameters expected in following order
// inputpath outputpath startYear endYear asin
public class SeasonalPeaksDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // will recieve year range and product id in parametrs do conf.set here

        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 5) {
            System.err.println("Usage:seasonal peak <in> [<in>...] <out>");
            System.exit(2);
        }

        conf.set("startYear",otherArgs[2]);
        conf.set("endYear",otherArgs[3]);
        conf.set("asin",otherArgs[4]);
        Job job = new Job(conf, "driver");
        job.setJarByClass(SeasonalPeaksDriver.class);
        job.setMapperClass(SeasonalPeaksMapper.class);
        //job.setCombinerClass(SeasonalPeaksReducer.class);
        job.setReducerClass(SeasonalPeaksReducer.class);
        job.setPartitionerClass(SeasonalPeaksPartitioner.class);
        job.setOutputKeyClass(DateProductWritable.class);
        job.setOutputValueClass(RatingCountWritable.class);
        job.setMapOutputKeyClass(DateProductWritable.class);
        job.setMapOutputValueClass(RatingCountWritable.class);
        job.setNumReduceTasks(12);// should come into parameters
        FileInputFormat.setInputDirRecursive(job, true);
        //for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //}

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
