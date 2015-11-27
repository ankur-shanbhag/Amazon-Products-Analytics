package edu.neu.mr.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DateProductWritable implements Writable {
	private long date;
	private String asin;

	public DateProductWritable() {
		date = 0;
		// asin = "";
	}

	public String getAsin() {
		return asin;
	}

	public void setAsin(String asin1) {
		asin = asin1;
	}

	public long getDate() {
		return date;
	}

	public void setDate(long date1) {
		date = date1;
	}

	public void readFields(DataInput in) throws IOException {
		date = in.readLong();
		asin = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(date);
		out.writeUTF(asin);
	}

	public String toString() {
		return date + "," + asin;
	}

}
