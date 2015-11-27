/**
 * Created by vjain on 11/26/15.
 */

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SeasonalPeakMapReduce {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage:seasonal peak <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "airline filter");
        job.setJarByClass(SeasonalPeakMapReduce.class);
        job.setMapperClass(SeasonalPeakMapReduce.SeasonalPeaksMapper.class);

        job.setReducerClass(SeasonalPeakMapReduce.SeasonalPeaksReducer.class);
        job.setOutputKeyClass(DateProductWritable.class);
        job.setOutputValueClass(RatingCountWritable.class);
        job.setMapOutputKeyClass(DateProductWritable.class);
        job.setMapOutputValueClass(RatingCountWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class SeasonalPeaksMapper extends Mapper<Object, Text, DateProductWritable, RatingCountWritable> {

        // setting all the parameters for flight filtering
        private DateProductWritable outTupleKey;
        private RatingCountWritable outTupleVal;
        private long START_YEAR = 2007;
        private long END_YEAR = 2009;
        private String TARGET_ASIN = "";

        public SeasonalPeaksMapper() throws ParseException {
        }

        // map output has a key of destination and value as the FlightData custom writable
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            DateProductWritable outKeyYear = Utility.getWritableKey(value.toString(), false);
            DateProductWritable outKeyMonth = Utility.getWritableKey(value.toString(), true);
            RatingCountWritable outValue = Utility.getWritableValue(value.toString());

            if (keepDataPoint(outKeyYear)) {
                context.write(outKeyYear, outValue);
                context.write(outKeyMonth, outValue);
            }
        }


        private Boolean keepDataPoint(DateProductWritable datapoint) {

            if (datapoint.getDate() >= START_YEAR && datapoint.getDate() <= END_YEAR && datapoint.getAsin().equals(TARGET_ASIN))
                return true;
            else
                return false;
        }

    }

    public static class SeasonalPeaksReducer extends Reducer<DateProductWritable, RatingCountWritable, DateProductWritable, RatingCountWritable> {
        // reduce basically joins data by putting flights originating from ORD tin first leg and reaching JFK in second leg
        public RatingCountWritable outResult = new RatingCountWritable();
        public void reduce(DateProductWritable key, Iterable<RatingCountWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;

            for(RatingCountWritable item : values){
                sum += item.getOverall() * item.getCount();
                count += item.getCount();
            }

            outResult.setOverall(sum/count);
            outResult.setCount(count);

            context.write(key,outResult);
        }
    }
}
