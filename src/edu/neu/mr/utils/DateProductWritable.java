package edu.neu.mr.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DateProductWritable implements WritableComparable<DateProductWritable> {
	private long date;
	private String asin;

	public DateProductWritable() {
		date = 0;
		asin = "";
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

	public int compareTo(DateProductWritable o) {
		String thisAsin = this.getAsin();
		String thatAsin = o.getAsin();
		long thisDate = this.getDate();
		long thatDate = o.getDate();

		return (thisDate < thatDate ? -1 : (thisDate==thatDate? 0 : 1));
	}

	/*public boolean equals(DateProductWritable o) {
		String thisAsin = this.getAsin();
		String thatAsin = o.getAsin();
		long thisDate = this.getDate();
		long thatDate = o.getDate();

		return (thisDate == thatDate && thisAsin == thatAsin);
	}*/

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof DateProductWritable)) return false;

		DateProductWritable that = (DateProductWritable) o;

		if (getDate() != that.getDate()) return false;
		return getAsin().equals(that.getAsin());

	}

	@Override
	public int hashCode() {
		int result = (int) (getDate() ^ (getDate() >>> 32));
		result = 31 * result + getAsin().hashCode();
		return result;
	}

	public String toString() {
		return date + "," + asin;
	}

}
