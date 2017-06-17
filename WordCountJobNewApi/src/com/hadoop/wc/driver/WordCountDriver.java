package com.hadoop.wc.driver;

// In the new API classes are in the "mapreduce" package
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hadoop.wc.mapper.WordCountMapper;
import com.hadoop.wc.reducer.WordCountReducer;

// In the new API no need to implement the Tool interface
// and hence no need provide implementation for the run() method of Tool interface
public class WordCountDriver extends Configured {

	public static void main(String[] args) throws Exception {

		// loading the configuration
		Configuration conf = new Configuration(Boolean.TRUE);

		// Setting the configurations ..
		// -----------------------------
		// Job name (Optional)
		// InputPaths - file(s)
		// OutputPath - directory
		// InputFormat (Optional)
		// OutputFormat (Optional)
		// Mapper Class
		// Reducer Class
		// OutputKeyClass
		// OutputValueClass
		
		Job job = Job.getInstance(conf, "wordcount-job");
		
		job.setJarByClass(WordCountDriver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		//By default it is TextInputFormat and TextOutputFormat, if not set.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//setInputPath(s) since multiple input paths can be given as argumetns
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//But output path can only be one
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// waitForCompletion() is trigger method in the new API, while JobClient.runJob() was used in the old API.
		job.waitForCompletion(true);
	}

}
