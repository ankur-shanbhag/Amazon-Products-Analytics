package edu.neu.mr.amazon.reviews;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import edu.neu.mr.amazon.reviews.utils.Constants;
import edu.neu.mr.amazon.reviews.utils.WritableParent;

public class ProductMetadataMapper extends Mapper<LongWritable, Text, Text, WritableParent> {

	private Text asin = new Text();
	private Text metadata = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		JSONObject object = null;
		try{
			object = new JSONObject(value.toString());
			asin.set(object.getString(Constants.ASIN));
			String title = object.optString(Constants.TITLE);
			String price = object.optString(Constants.PRICE);
			String brand = object.optString(Constants.BRAND);
			
			Object related = object.opt(Constants.RELATED);
			if(related == null)
				related = new JSONObject();
			JSONObject relatedJSON = (JSONObject) related;
			String alsoBought = removeExtraBrackets(relatedJSON.optString(Constants.ALSO_BOUGHT));
			String alsoViewed = removeExtraBrackets(relatedJSON.optString(Constants.ALSO_VIEWED));
			String boughtTogether = removeExtraBrackets(relatedJSON.optString(Constants.BOUGHT_TOGETHER));
			metadata.set(title+"|"+price+"|"+brand+"|"+alsoBought+"|"+alsoViewed+"|"+boughtTogether);
			context.write(asin, new WritableParent(metadata));
		}
		catch(JSONException exception){
			System.out.println(object);
			exception.printStackTrace();
		}
	}

	private String removeExtraBrackets(String optString) {
		if(optString == null || optString.trim().equals("") || optString.length() <= 2)
			return "";
		return optString.substring(1, optString.length()-1);
	}
}

