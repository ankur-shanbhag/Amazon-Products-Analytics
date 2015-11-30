package edu.neu.mr.seasonalPeaks;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.neu.mr.utils.DateProductWritable;
import edu.neu.mr.utils.RatingCountWritable;
import edu.neu.mr.utils.Utility;

public class SeasonalPeaksMapper extends Mapper<Object, Text, DateProductWritable, RatingCountWritable> {

    // setting all the parameters for flight filtering
    private long START_YEAR = 2013;
    private long END_YEAR = 2014;
    private String TARGET_ASIN = "120401325X";
    
    private Map<DateProductWritable,RatingCountWritable> localMap = new HashMap<DateProductWritable,RatingCountWritable>();
    
    public void setup(){
		// no task initiation needed
	}

    // map output has a key of destination and value as the FlightData custom writable
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        DateProductWritable outKeyYear = Utility.getWritableKey(value.toString(), false);
        DateProductWritable outKeyMonth = Utility.getWritableKey(value.toString(), true);
        RatingCountWritable outValue = Utility.getWritableValue(value.toString());

    /*    if (keepDataPoint(outKeyYear)) {
            context.write(outKeyYear, outValue);
            context.write(outKeyMonth, outValue);
        }*/
        
        if(outKeyYear !=null && outKeyMonth != null && outValue != null && keepDataPoint(outKeyYear)){
        	RatingCountWritable currValue = localMap.get(outKeyMonth);
			if (currValue != null) {
				//updating the overall average
				double newOverall = ((currValue.getOverall() * currValue.getCount()) + outValue.getOverall())/(currValue.getCount() + 1);
				//updating the count of the item in sold in the given month
				int newCount = currValue.getCount() + 1;
				currValue.setCount(newCount);
				currValue.setOverall(newOverall);
				localMap.put(outKeyMonth, currValue); // updating the count of each word encountered thus far
			} else {
				localMap.put(outKeyMonth, outValue);
			}
        }
    }


    private Boolean keepDataPoint(DateProductWritable datapoint) {
        if (datapoint.getDate() >= START_YEAR && datapoint.getDate() <= END_YEAR && datapoint.getAsin().equals(TARGET_ASIN))
            return true;
        else
            return false;
    }
    
    public void cleanup(Context context){
		for (Entry<DateProductWritable, RatingCountWritable> entry : localMap.entrySet()) {
			DateProductWritable outKeyMonth = entry.getKey();
			RatingCountWritable outValue = entry.getValue();
			try {
				context.write(outKeyMonth, outValue);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
