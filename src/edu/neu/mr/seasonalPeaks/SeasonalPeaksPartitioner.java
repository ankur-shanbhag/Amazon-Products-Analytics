package edu.neu.mr.seasonalPeaks;

/**
 * Created by vjain on 11/30/15.
 */

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import edu.neu.mr.utils.DateProductWritable;
import edu.neu.mr.utils.RatingCountWritable;
import edu.neu.mr.utils.Utility;

public class SeasonalPeaksPartitioner extends Partitioner<DateProductWritable,RatingCountWritable>{
    public int getPartition(DateProductWritable key, RatingCountWritable value,int numberParts){
        int month = (int) key.getDate() % 100;


        if(numberParts == 12) {
            switch (month){
                case 1:
                    return 0;
                case 2:
                    return 1;
                case 3:
                    return 2;
                case 4:
                    return 3;
                case 5:
                    return 4;
                case 6:
                    return 5;
                case 7:
                    return 6;
                case 8:
                    return 7;
                case 9:
                    return 8;
                case 10:
                    return 9;
                case 11:
                    return 10;
                case 12:
                    return 11;
                default:
                    return 0;

            }
        }
        else{
            return 0;
        }
    }
}
