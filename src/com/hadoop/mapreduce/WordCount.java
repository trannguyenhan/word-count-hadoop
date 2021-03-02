package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	public static void main(String[] args) throws Exception {
		String in = args[0];
		String out = args[1];
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Word count");
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WCMapper.class);
		job.setCombinerClass(WCReducer.class);
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(in));
		FileSystem fs = FileSystem.get(conf); // delete file output when it exists
		if (fs.exists(new Path(out))) {
			fs.delete(new Path(out), true);
		}
		
		FileOutputFormat.setOutputPath(job, new Path(out));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}