package edu.neu.mr.utils;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class Utility {


    // this function reads the data.csv and selects the key fields from the file and initializes the object.
    public static DateProductWritable getWritableKey(String dataPoint,boolean month){
        boolean validDataPoint = true;

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
        fData.setAsin(String.valueOf(comaSplits[0]));

        // if date is not long then row is invalid
        try {
            Long.parseLong(comaSplits[2]);
            }
        catch (Exception ex){
            validDataPoint = false;
        }

        if(validDataPoint) {
            fData.setDate(Long.valueOf(getDate(comaSplits[2], month)));
            return fData;
        }
        else{
            return null;
        }
    }


    // used for custom csv parser.
    static boolean toggleQuote(boolean currentState){
        if(currentState)
            return false;
        else
            return true;
    }

    // converts unix epoch time to date and extracts yyyy or yyyyMM from date
    public static long getDate(String date,boolean month){
        long dateTemp = Long.parseLong(date) * 10000;
        Date reviewDate = new Date(dateTemp);
        DateFormat df;
        if(month) {
            df = new SimpleDateFormat("yyyyMM");
            return Long.parseLong(df.format(reviewDate));
        }
        else {
            df = new SimpleDateFormat("yyyy");
            return Long.parseLong(df.format(reviewDate));
        }
    }

    // this function creates the value for each asin,date key
    public static RatingCountWritable getWritableValue(String dataPoint){
        boolean validDataPoint = true;
        RatingCountWritable fdd = new RatingCountWritable();
        String [] comasplit = dataPoint.split(",");
        fdd.setCount(1);
        // if average is not double then row is invalid
        try {
            Double.parseDouble(comasplit[1]);
        }
        catch (Exception ex){
            validDataPoint = false;
        }

        if(validDataPoint) {
            fdd.setOverall(Double.valueOf(comasplit[1]));
            return fdd;
        }
        else {
            return  null;
        }
    }
}
