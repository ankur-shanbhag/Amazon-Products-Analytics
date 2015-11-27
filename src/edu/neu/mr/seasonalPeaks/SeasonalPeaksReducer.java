package edu.neu.mr.seasonalPeaks;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import edu.neu.mr.utils.DateProductWritable;
import edu.neu.mr.utils.RatingCountWritable;

public class SeasonalPeaksReducer extends Reducer<DateProductWritable, RatingCountWritable, DateProductWritable, RatingCountWritable> {
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