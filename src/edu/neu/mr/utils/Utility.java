package edu.neu.mr.utils;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class Utility {


    // this function reads the data.csv and selects the key fields from the file and initializes the FlightData object.
    public static DateProductWritable getWritableKey(String dataPoint,boolean month){

        StringBuilder sb = new StringBuilder();
        boolean doubleQuoteOpen = false;
        String toAdd = "";
        for(int i = 0 ; i < dataPoint.length() ; i++){
            if(doubleQuoteOpen){
                if(dataPoint.charAt(i) == ',')
                    toAdd = " ";
                else
                    toAdd = String.valueOf(dataPoint.charAt(i));
            }
            else
                toAdd = String.valueOf(dataPoint.charAt(i));

            if(dataPoint.charAt(i) == '"') {
                doubleQuoteOpen = toggleQuote(doubleQuoteOpen);
                toAdd = "";
            }

            sb.append(toAdd);
        }


        String [] comaSplits = sb.toString().split(",");
        DateProductWritable fData = new DateProductWritable();
        int i=0;
        fData.setAsin(String.valueOf(comaSplits[0]));
        fData.setDate(Long.valueOf(getDate(comaSplits[2],month)));
        return fData;
    }


    // used for custom csv parser.
    static boolean toggleQuote(boolean currentState){
        if(currentState)
            return false;
        else
            return true;
    }


    public static long getDate(String date,boolean month){
        Date reviewDate = new Date(Long.parseLong(date));
        DateFormat df;
        if(month) {
            df = new SimpleDateFormat("yyyyMMdd");
            return Long.parseLong(df.format(reviewDate));
        }
        else {
            df = new SimpleDateFormat("yyyy");
            return Long.parseLong(df.format(reviewDate));
        }
    }

    // this function reads the output form first map reduce and sets the FlightAggregation object with count and avegare
    public static RatingCountWritable getWritableValue(String dataPoint){
        RatingCountWritable fdd = new RatingCountWritable();
        String [] comasplit = dataPoint.split(",");
        fdd.setCount(1);
        fdd.setOverall(Double.valueOf(comasplit[1]));
        return fdd;
    }
}
