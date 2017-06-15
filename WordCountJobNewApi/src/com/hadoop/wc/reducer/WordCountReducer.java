package com.hadoop.wc.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		//super.reduce(arg0, arg1, arg2);
		int sum = 0;
		while (values.iterator().hasNext()) {
			sum = sum + values.iterator().next().get();
		}
		context.write(key, new IntWritable(sum));
	}
}
