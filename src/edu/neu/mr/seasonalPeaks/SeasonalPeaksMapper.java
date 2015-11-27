package edu.neu.mr.seasonalPeaks;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.neu.mr.utils.DateProductWritable;
import edu.neu.mr.utils.RatingCountWritable;
import edu.neu.mr.utils.Utility;

public class SeasonalPeaksMapper extends Mapper<Object, Text, DateProductWritable, RatingCountWritable> {

    // setting all the parameters for flight filtering
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
