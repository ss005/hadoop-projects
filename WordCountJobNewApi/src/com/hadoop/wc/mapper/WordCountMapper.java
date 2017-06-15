package com.hadoop.wc.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Text, Iterable<IntWritable>, Text, IntWritable> {
	@Override
	protected void map(Text key, Iterable<IntWritable> value,
			Mapper<Text, Iterable<IntWritable>, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
			//super.map(key, value, context);
		
		String line = value.toString();
		StringTokenizer token = new StringTokenizer(line);
		while (token.hasMoreTokens()) {
			String word = token.nextToken();
			context.write(new Text(word), new IntWritable (1));
		}
	}
}
