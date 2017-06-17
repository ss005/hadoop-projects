package com.hadoop.wc.driver;

// classes are in "mapred" package in old API
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.wc.mapper.WordCountMapper;
import com.hadoop.wc.reducer.WordCountReducer;

// Tool interface needs to be implemented in old API 
public class WordCountDriver extends Configured implements Tool {


	public static void main(String[] args) {

		// Step 1 : validating the input
		if (args.length < 2) {
			System.out.println("Usage : hadoop jar " + WordCountDriver.class.getName() + "input/localEdgeNoteFile/path"
					+ "output/directory/path");
			System.exit(1);
		}

		// Step 2: loading default configurations
		Configuration conf = new Configuration(Boolean.TRUE);

		int result = 0;
		try {
		// Step 3 : calling run method of ToolRunner
			result = ToolRunner.run(conf, new WordCountDriver(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (result == 0) {
			System.out.println("SUCCESS");
		} else {
			System.out.println("FAILURE");
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		JobConf jobConf = new JobConf(WordCountDriver.class);
		
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
		
		jobConf.setJobName("wordcount-job");

		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		
		// default is TextInputFormat and TextOutputFormat if not written 
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		
		jobConf.setMapperClass(WordCountMapper.class);
		jobConf.setReducerClass(WordCountReducer.class);
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		
		// submit the job
		// trigger method is JobClient.runJob() in old API to submit the job.
		JobClient.runJob(jobConf);
		
		return 0;
	}

}
