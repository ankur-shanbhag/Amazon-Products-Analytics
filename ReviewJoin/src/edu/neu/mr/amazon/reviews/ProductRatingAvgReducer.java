package edu.neu.mr.amazon.reviews;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.neu.mr.amazon.reviews.utils.Constants;
import edu.neu.mr.amazon.reviews.utils.RatingPairWritable;
import edu.neu.mr.amazon.reviews.utils.WritableParent;

public class ProductRatingAvgReducer extends Reducer<Text, WritableParent, Text, Text> {

	Text grouppedMetadata = new Text();
	String category;

	@Override
	protected void setup(Reducer<Text, WritableParent, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		Configuration configuration = context.getConfiguration();
		category = configuration.get(Constants.CATEGORY);
	}

	@Override
	public void reduce(Text key, Iterable<WritableParent> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0.0;
		int count = 0;
		double avg = 0;

		String metadata = "";
		for (WritableParent writableParent : values) {
			Writable writable = writableParent.get();
			if (writable instanceof Text) {
				metadata = ((Text) writable).toString();
			} else {
				RatingPairWritable rating = (RatingPairWritable) writable;
				sum += rating.getRatingSum().get();
				count += rating.getRatingCount().get();
			}
		}

		if (count != 0) {
			avg = sum / count;
		}
		grouppedMetadata.set(metadata + Constants.FIELD_SEPRATOR + category + Constants.FIELD_SEPRATOR
				+ new DecimalFormat(Constants.DECIMAL_FORMAT).format(avg) + Constants.FIELD_SEPRATOR + count);
		context.write(key, grouppedMetadata);
	}
}
