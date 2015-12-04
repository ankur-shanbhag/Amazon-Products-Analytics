package edu.neu.mr.amazon.reviews;


import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.neu.mr.amazon.reviews.utils.Constants;
import edu.neu.mr.amazon.reviews.utils.WritableParent;

public class ReviewMetdataJoinDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set(Constants.KEY_VALUE_SEPRATOR_PROPERTY, Constants.FIELD_SEPRATOR);
		if (args.length < 2) {
			System.err.println("Usage: ReviewMetdataJoinDriver <in dir> <out dir>");
			System.exit(2);
		}

		File directory = new File(args[0]);
		if(!directory.isDirectory()){
			System.err.println("Input path is not directory");
			System.exit(3);
		}

		for(String categoryPath:directory.list()){
			String fullCurrentPath = directory+File.separator+categoryPath;
			File categoryDir = new File(fullCurrentPath);
			if(!categoryDir.isDirectory())
				continue;
			conf.set(Constants.CATEGORY, categoryPath);
			Job job = new Job(conf, "Review and Metadata equi-join for category"+categoryPath);

			job.setJarByClass(ReviewMetdataJoinDriver.class);
			for(String jsonFile: categoryDir.list()){
				Path path = new Path(fullCurrentPath+File.separator+jsonFile);
				if(jsonFile.startsWith(Constants.METADATA_PREFIX))
					MultipleInputs.addInputPath(job,path,TextInputFormat.class,ProductMetadataMapper.class);
				else if(jsonFile.startsWith(Constants.REVIEW_PREFIX))
					MultipleInputs.addInputPath(job,path,TextInputFormat.class,ProductRatingMapper.class);
			}
			job.setCombinerClass(ProductRatingAvgCombiner.class);
			job.setReducerClass(ProductRatingAvgReducer.class);
			FileOutputFormat.setOutputPath(job, new Path(args[1]+File.separator+Constants.CATEGORY_EQUAL+categoryPath));
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(WritableParent.class);
			job.setNumReduceTasks(1);
			job.submit();
		}
	}
}
