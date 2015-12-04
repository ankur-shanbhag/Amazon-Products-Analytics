package edu.neu.mr.amazon.reviews;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.neu.mr.amazon.reviews.utils.RatingPairWritable;
import edu.neu.mr.amazon.reviews.utils.WritableParent;

public class ProductRatingAvgCombiner extends Reducer<Text, WritableParent, Text, WritableParent> {

	Text grouppedMetadata = new Text();
	RatingPairWritable rPairWritable = new RatingPairWritable();

	@Override
	public void reduce(Text key, Iterable<WritableParent> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0.0;
		int count = 0;
		
		for (WritableParent writableParent : values){
		    Writable writable = writableParent.get();
			if (writable instanceof Text){
				context.write(key, writableParent);
			}
			else{
				RatingPairWritable rating = (RatingPairWritable) writable;
				sum += rating.getRatingSum().get();
				count += rating.getRatingCount().get();
			}
		}
		
		rPairWritable.setRatingCount(new IntWritable(count));
		rPairWritable.setRatingSum(new DoubleWritable(sum));
		context.write(key, new WritableParent(rPairWritable));
		}
	}

