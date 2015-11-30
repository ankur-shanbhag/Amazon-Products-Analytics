package edu.neu.mr.utils;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

// this class is used to map the json file to csv using the JsonToCSVWithMetadata

@JsonIgnoreProperties(ignoreUnknown = true)
public class JoinedData {
    private String asin;
    private Double overall;
    private long unixReviewTime;

    public String getAsin(){
        return asin;
    }

    public Double getOverall(){
        return overall;
    }

    public long getUnixReviewTime(){
        return unixReviewTime;
    }


    public void setAsin(String asin1){
        asin = asin1;
    }

    public void setOverall(Double overall1){
        overall = overall1;
    }

    public void setUnixReviewTime(long unixReviewTime1){
        unixReviewTime = unixReviewTime1;
    }

}
