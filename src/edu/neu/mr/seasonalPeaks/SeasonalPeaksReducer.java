package edu.neu.mr.seasonalPeaks;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import edu.neu.mr.utils.DateProductWritable;
import edu.neu.mr.utils.RatingCountWritable;


// output of reducer is DateProductWritable, RatingCountWritable and input is also DateProductWritable, RatingCountWritable
// this methods computes the average overall rating and count of the product for the given month
// and writes in the context
public class SeasonalPeaksReducer extends Reducer<DateProductWritable, RatingCountWritable, DateProductWritable, RatingCountWritable> {
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