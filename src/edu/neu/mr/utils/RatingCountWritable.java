package edu.neu.mr.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


// this is writable for map output value and contains average overall rating and count
public class RatingCountWritable implements Writable{
    private int count;
    private double overall;

    public RatingCountWritable(){
        count = 0;
        overall = 0;
    }

    public int getCount(){
        return count;
    }

    public double getOverall(){
        return overall;
    }

    public void setCount(int cnt){
        count = cnt;
    }

    public void setOverall(double overall1){
        overall = overall1;
    }

    public void readFields(DataInput in) throws IOException{
        overall = in.readDouble();
        count = in.readInt();
    }

    public void write(DataOutput out) throws IOException{
        out.writeDouble(overall);
        out.writeInt(count);
    }

    public  String toString(){
        return String.valueOf(overall)+ "," + String.valueOf(count);
    }

}
