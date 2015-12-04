package edu.neu.mr.amazon.reviews.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class RatingPairWritable implements Writable{
	private DoubleWritable ratingSum;
	private IntWritable ratingCount;
	
	public RatingPairWritable() {
		this.ratingCount = new IntWritable();
		this.ratingSum = new DoubleWritable();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		ratingSum.readFields(in);
		ratingCount.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		ratingSum.write(out);
		ratingCount.write(out);
	}

	public IntWritable getRatingCount() {
		return ratingCount;
	}

	public void setRatingCount(IntWritable ratingCount) {
		this.ratingCount = ratingCount;
	}

	public DoubleWritable getRatingSum() {
		return ratingSum;
	}

	public void setRatingSum(DoubleWritable ratingSum) {
		this.ratingSum = ratingSum;
	}
	@Override
	public String toString() {
		return ratingSum.toString()+"|"+ratingCount.toString();
	}
}
