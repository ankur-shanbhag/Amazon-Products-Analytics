package edu.neu.mr.amazon.reviews;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import edu.neu.mr.amazon.reviews.utils.RatingPairWritable;
import edu.neu.mr.amazon.reviews.utils.WritableParent;

public class ProductRatingMapper extends Mapper<LongWritable, Text, Text, WritableParent> {

	private Text asin = new Text();
	private RatingPairWritable rating = new RatingPairWritable();
	private static final IntWritable ONE = new IntWritable(1);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try{
			JSONObject object = new JSONObject(value.toString());
			asin.set(object.getString("asin"));
			rating.setRatingSum(new DoubleWritable(object.optDouble("overall")));
			rating.setRatingCount(ONE);
			context.write(asin, new WritableParent(rating));
		}
		catch(JSONException exception){
			exception.printStackTrace();
		}
	}
}
